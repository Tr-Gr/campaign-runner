# Databricks notebook source
# Campaign Runner: shared library. Loaded by the other notebooks with %run ./pco_lib
#
# Each function below maps 1:1 to a stage of the current campaign pipeline, so the shared modules stay shared
# and only the campaign-specific parts (the audience rule and optional segmentation) live in campaign_config:
#
#   audience()               1.  Audience query (campaign-specific rule from campaign_config)
#   lookbacks()              2a. Lookbacks: remove people sent recently
#   target_to_recipient()    2b. Target to recipient: resolve the contact for the channel, dedupe
#   standard_exclusions()    2c. Standard exclusions
#   build_payload()          2d. Write JSON payload: personalization fields for comms
#   segmentation()           2e. Segmentation (campaign-specific, optional)
#   schedule_messages()      2f. Message approved scheduler: send window and expiration
#   write_message_approved() 3.  message_approved (MERGE on message_guid, idempotent per campaign and run date)
#
# Audience and segmentation rules are SQL predicates validated against the columns of the audience_source view
# (no functions, no subqueries, no semicolons) before they touch Spark.

# COMMAND ----------

import re
import json
import random
import time as _time
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from croniter import croniter
from pyspark.sql import functions as F
from pyspark.sql import Window

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "campaigns")
dbutils.widgets.text("table_prefix", "camp_")
CAT = dbutils.widgets.get("catalog").strip()
SCH = dbutils.widgets.get("schema").strip()
P = dbutils.widgets.get("table_prefix").strip()
assert CAT and SCH, "Set the catalog and schema parameters (the bundle passes them from databricks.yml)"
TZ = ZoneInfo("America/Chicago")


def T(name):
    return f"{CAT}.{SCH}.{P}{name}"


def today_ct():
    return datetime.now(TZ).date()


def parse_run_date(value):
    value = (value or "").strip()
    return date.fromisoformat(value) if value else today_ct()


# COMMAND ----------

# Rule validation: whitelist = the columns of the audience_source view, so redefining the view is all it takes
# to make new attributes available to campaign rules.
SQL_KEYWORDS = {"AND", "OR", "NOT", "IN", "IS", "NULL", "TRUE", "FALSE", "BETWEEN", "LIKE"}
_TOKEN_RE = re.compile(
    r"\s*(?:(?P<str>'(?:[^']|'')*')|(?P<num>-?\d+(?:\.\d+)?)|(?P<op><>|!=|>=|<=|=|<|>)"
    r"|(?P<lp>\()|(?P<rp>\))|(?P<comma>,)|(?P<id>[A-Za-z_][A-Za-z0-9_]*))"
)


def audience_columns():
    return set(spark.table(T("audience_source")).columns)


def validate_rule(rule, allowed):
    """Accept only whitelisted columns, SQL keywords, literals, comparison operators, parentheses and commas."""
    if not rule or len(rule) > 2000:
        raise ValueError("rule is empty or too long")
    s, pos, toks = rule.strip(), 0, []
    while pos < len(s):
        m = _TOKEN_RE.match(s, pos)
        if not m:
            raise ValueError(f"disallowed syntax in rule near: {s[pos:pos + 25]!r}")
        pos = m.end()
        kind, val = m.lastgroup, m.group(m.lastgroup)
        if kind == "id":
            up = val.upper()
            if up in SQL_KEYWORDS:
                toks.append(("kw", up))
            elif val in allowed:
                toks.append(("col", val))
            else:
                raise ValueError(f"column not allowed in rule: {val}")
        else:
            toks.append((kind, val))
    for i, (k, v) in enumerate(toks[:-1]):
        if k == "col" and toks[i + 1][0] == "lp":
            raise ValueError(f"function-style call not allowed: {v}(")
    return rule


# COMMAND ----------

# Scheduling (Quartz cron, Central time)
def quartz_to_cron5(expr):
    f = expr.split()
    if len(f) == 6:
        f = f[1:]
    minute, hour, dom, month, dow = f
    return f"{minute} {hour} {'*' if dom == '?' else dom} {month} {'*' if dow == '?' else dow}"


def campaign_send_times(c, d):
    """Send datetimes (Central) for campaign dict c on day d. Empty if not due."""
    if not c["active"] or d < c["start_date"] or (c["end_date"] is not None and d > c["end_date"]):
        return []
    f = c["schedule_cron"].split()
    minute, hour = int(f[1]), int(f[2])
    if c["campaign_type"] == "one_time":
        return [datetime.combine(d, time(hour, minute))] if d == c["start_date"] else []
    it = croniter(quartz_to_cron5(c["schedule_cron"]), datetime.combine(d, time(0)) - timedelta(seconds=1))
    out = []
    while True:
        nxt = it.get_next(datetime)
        if nxt.date() != d:
            break
        out.append(nxt)
    return out


def is_approved(c):
    return (not c["requires_approval"]) or bool(c.get("approved_by"))


def active_campaigns():
    return [r.asDict() for r in spark.table(T("campaign_config")).where("active").collect()]


def build_calendar(as_of, days=30):
    """Rebuild the calendar window [as_of, as_of + days) from campaign_config."""
    from pyspark.sql import types as Tp
    now, end, rows = datetime.now(), as_of + timedelta(days=days), []
    for c in active_campaigns():
        for i in range(days):
            d = as_of + timedelta(days=i)
            for t in campaign_send_times(c, d):
                rows.append((d, t.strftime("%H:%M"), c["campaign_id"], c["campaign_name"], c["channel"],
                             c["priority_class"], c["owner_team"], int(c["max_per_run"] or 0),
                             bool(is_approved(c)), now))
    schema = Tp.StructType([
        Tp.StructField("send_date", Tp.DateType()), Tp.StructField("send_time_ct", Tp.StringType()),
        Tp.StructField("campaign_id", Tp.StringType()), Tp.StructField("campaign_name", Tp.StringType()),
        Tp.StructField("channel", Tp.StringType()), Tp.StructField("priority_class", Tp.StringType()),
        Tp.StructField("owner_team", Tp.StringType()), Tp.StructField("max_pieces", Tp.LongType()),
        Tp.StructField("approved", Tp.BooleanType()), Tp.StructField("refreshed_at", Tp.TimestampType()),
    ])
    spark.sql(f"DELETE FROM {T('calendar')} WHERE send_date >= DATE'{as_of}' AND send_date < DATE'{end}'")
    if rows:
        spark.createDataFrame(rows, schema).write.mode("append").saveAsTable(T("calendar"))
    return len(rows)


def with_retry(fn, tries=8, label="write"):
    """Concurrent campaign tasks write to shared tables; Delta conflicts are transient, so retry with backoff."""
    for i in range(tries):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            msg = str(e).upper()
            if ("CONCURRENT" in msg or "CONFLICT" in msg) and i < tries - 1:
                _time.sleep(random.uniform(1, 3) * (i + 1))
                continue
            raise


# COMMAND ----------

# 1. Audience query (campaign-specific)
def audience(cfg, allowed):
    rule = validate_rule(cfg["audience_rule"], allowed)
    return spark.table(T("audience_source")).where(rule)


# 2a. Lookbacks: remove people who already received this campaign (or channel) within lookback_days.
# Uses send_history, which outlives message_approved retention, so lookbacks never depend on the cleanup schedule.
def lookbacks(df, cfg, run_date):
    days = int(cfg["lookback_days"] or 0)
    if days <= 0:
        return df
    scope = (cfg["lookback_scope"] or "campaign").lower()
    hist = spark.table(T("send_history")).where(
        f"sent_date >= DATE'{run_date - timedelta(days=days)}' AND sent_date < DATE'{run_date}'")
    if scope == "campaign":
        hist = hist.where(F.col("campaign_id") == cfg["campaign_id"])
    elif scope == "channel":
        hist = hist.where(F.col("channel") == cfg["channel"])
    return df.join(hist.select("person_id").distinct(), "person_id", "left_anti")


# 2b. Target to recipient: pick the contact point for the channel, require opt-in, dedupe people and addresses.
CHANNEL_CONTACT = {"email": ("email", "email_opt_in"), "text": ("mobile_phone", "sms_opt_in"),
                   "mail": ("mailing_address", "mail_opt_in")}


def target_to_recipient(df, cfg):
    addr_col, opt_col = CHANNEL_CONTACT[cfg["channel"]]
    rcp = spark.table(T("recipient_source")).select(
        "person_id", F.col(addr_col).alias("recipient_address"), F.col(opt_col).alias("opted_in"),
        "do_not_contact", "deceased_flag")
    out = (df.join(rcp, "person_id", "inner")
           .where("recipient_address IS NOT NULL AND opted_in"))
    w_person = Window.partitionBy("person_id").orderBy("recipient_address")
    w_addr = Window.partitionBy("recipient_address").orderBy("person_id")
    return (out.withColumn("_rp", F.row_number().over(w_person)).where("_rp = 1")
               .withColumn("_ra", F.row_number().over(w_addr)).where("_ra = 1")
               .drop("_rp", "_ra", "opted_in"))


# 2c. Standard exclusions applied to every campaign: deceased, do-not-contact, global suppression list.
def standard_exclusions(df):
    sup = spark.table(T("suppression_list")).select("person_id").distinct()
    return (df.where("NOT coalesce(deceased_flag, false) AND NOT coalesce(do_not_contact, false)")
              .join(sup, "person_id", "left_anti")
              .drop("deceased_flag", "do_not_contact"))


# 2d. Write JSON payload: the personalization fields the template asks for (rendering happens downstream).
def build_payload(df, cfg, tpl):
    fields = [f.strip() for f in (tpl["payload_fields"] or "").split(",") if f.strip()]
    missing = [f for f in fields if f not in df.columns]
    if missing:
        raise ValueError(f"template {tpl['template_id']} asks for fields not in audience_source: {missing}")
    struct = F.struct(*[F.col(f) for f in fields],
                      F.lit(cfg["campaign_name"]).alias("campaign_name"),
                      F.lit(tpl["template_id"]).alias("template_id"),
                      F.lit(int(tpl["template_version"])).alias("template_version"))
    return (df.withColumn("payload", F.to_json(struct))
              .withColumn("template_id", F.lit(tpl["template_id"]))
              .withColumn("template_version", F.lit(int(tpl["template_version"]))))


# 2e. Segmentation (optional): JSON list of {"segment": name, "rule": predicate}; first match wins.
def segmentation(df, cfg, allowed):
    rules = json.loads(cfg["segmentation_rules"]) if cfg.get("segmentation_rules") else []
    if not rules:
        return df.withColumn("segment", F.lit("all"))
    expr = None
    for r in rules:
        cond = F.expr(validate_rule(r["rule"], allowed))
        expr = F.when(cond, F.lit(r["segment"])) if expr is None else expr.when(cond, F.lit(r["segment"]))
    return df.withColumn("segment", expr.otherwise(F.lit("other")))


# 2f. Message approved scheduler: send window from the campaign's send time, expiration after N days.
def schedule_messages(df, cfg, run_date):
    times = campaign_send_times(cfg, run_date)
    start = times[0] if times else datetime.combine(run_date, time(9, 0))
    end = start + timedelta(hours=int(cfg["send_window_hours"] or 8))
    expires = start + timedelta(days=int(cfg["expiration_days"] or 7))
    return (df.withColumn("send_window_start", F.lit(start).cast("timestamp"))
              .withColumn("send_window_end", F.lit(end).cast("timestamp"))
              .withColumn("expires_at", F.lit(expires).cast("timestamp")))


# 3. message_approved: deterministic message_guid per (campaign, run date, person), MERGE so reruns are safe.
#    Unchanged rows are left alone (no update, nothing to re-send). Changed rows go back to PENDING.
#    Rows for this campaign and run date that are no longer in the audience, and not yet egressed, are retracted.
MESSAGE_COLUMNS = ["message_guid", "campaign_id", "campaign_name", "run_date", "run_id", "person_id", "channel",
                   "recipient_address", "segment", "template_id", "template_version", "payload", "content_hash",
                   "send_window_start", "send_window_end", "expires_at"]


def finalize(df, cfg, run_date, run_id):
    cap = int(cfg["max_per_run"] or 0)
    out = (df.withColumn("campaign_id", F.lit(cfg["campaign_id"]))
             .withColumn("campaign_name", F.lit(cfg["campaign_name"]))
             .withColumn("run_date", F.lit(run_date).cast("date"))
             .withColumn("run_id", F.lit(run_id))
             .withColumn("channel", F.lit(cfg["channel"]))
             .withColumn("message_guid", F.sha2(F.concat_ws("|", F.lit(cfg["campaign_id"]),
                                                             F.lit(run_date.isoformat()), F.col("person_id")), 256))
             .withColumn("content_hash", F.sha2(F.concat_ws("|", "recipient_address", "segment", "payload",
                                                             F.col("template_version").cast("string"),
                                                             F.col("send_window_start").cast("string"),
                                                             F.col("expires_at").cast("string")), 256)))
    if cap > 0:
        out = out.orderBy("message_guid").limit(cap)
    return out.select(*MESSAGE_COLUMNS)


def write_message_approved(df, cfg, run_date):
    view = f"src_{re.sub('[^A-Za-z0-9]', '_', cfg['campaign_id'])}_{run_date.strftime('%Y%m%d')}"
    df.createOrReplaceTempView(view)
    cols = ", ".join(MESSAGE_COLUMNS)
    vals = ", ".join(f"s.{c}" for c in MESSAGE_COLUMNS)
    sets = ", ".join(f"{c} = s.{c}" for c in MESSAGE_COLUMNS if c != "message_guid")
    sql = f"""
        MERGE INTO {T('message_approved')} t
        USING {view} s ON t.message_guid = s.message_guid
        WHEN MATCHED AND t.content_hash <> s.content_hash THEN UPDATE SET
            {sets}, egress_status = 'PENDING', egress_batch_id = NULL, egressed_at = NULL,
            updated_ts = current_timestamp()
        WHEN NOT MATCHED THEN INSERT ({cols}, egress_status, created_ts, updated_ts)
            VALUES ({vals}, 'PENDING', current_timestamp(), current_timestamp())
        WHEN NOT MATCHED BY SOURCE AND t.campaign_id = '{cfg['campaign_id']}'
             AND t.run_date = DATE'{run_date.isoformat()}' AND t.egress_status = 'PENDING' THEN DELETE
    """
    m = with_retry(lambda: spark.sql(sql).collect()[0].asDict(), label="message_approved")
    return {"inserted": int(m.get("num_inserted_rows", 0)), "updated": int(m.get("num_updated_rows", 0)),
            "retracted": int(m.get("num_deleted_rows", 0))}
