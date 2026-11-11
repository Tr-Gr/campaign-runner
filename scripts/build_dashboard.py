"""Build the Campaign Operations dashboard (src/dashboards/campaign_operations.lvdash.json).
Table names are unqualified with the default prefix camp_; the bundle sets the catalog and schema
(dataset_catalog / dataset_schema). If you change table_prefix, change PREFIX here and rerun."""
import json
import os
import uuid

HERE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "dashboards")
PREFIX = "camp_"
WEEK_START = "date_trunc('week', current_date())"
WEEK_END = "date_add(date_trunc('week', current_date()), 6)"
NEXT30 = "send_date >= current_date() AND send_date < date_add(current_date(), 30)"

DATASETS = [
    ("ds_kpi", "KPIs", f"""
SELECT
  (SELECT COUNT(*) FROM camp_calendar WHERE send_date BETWEEN {WEEK_START} AND {WEEK_END}) AS sends_this_week,
  (SELECT COALESCE(SUM(max_pieces), 0) FROM camp_calendar WHERE send_date BETWEEN {WEEK_START} AND {WEEK_END}) AS pieces_this_week,
  (SELECT COUNT(*) FROM camp_calendar WHERE priority_class = 'regulatory' AND send_date >= current_date() AND send_date < date_add(current_date(), 14)) AS regulatory_next_14d,
  (SELECT COUNT(*) FROM camp_run_exceptions WHERE exception_state = 'FAILED') AS failed_sends,
  (SELECT COUNT(*) FROM camp_run_exceptions WHERE exception_state IN ('LATE', 'NOT_DELIVERED')) AS late_sends"""),
    ("ds_week", "This week's sends", f"""
SELECT send_date, send_time_ct, campaign_name, priority_class, channel, owner_team, max_pieces, approved
FROM camp_calendar
WHERE send_date BETWEEN {WEEK_START} AND {WEEK_END}
ORDER BY send_date, send_time_ct"""),
    ("ds_daily", "Sends by day and priority (next 30 days)", f"""
SELECT send_date, priority_class, SUM(max_pieces) AS pieces
FROM camp_calendar WHERE {NEXT30}
GROUP BY send_date, priority_class ORDER BY send_date"""),
    ("ds_channel", "Volume by channel (next 30 days)", f"""
SELECT channel, SUM(max_pieces) AS pieces, COUNT(*) AS sends
FROM camp_calendar WHERE {NEXT30}
GROUP BY channel ORDER BY pieces DESC"""),
    ("ds_type", "Delivery status by campaign (last 14 days)", """
SELECT campaign_name, egress_status, SUM(pieces) AS pieces
FROM camp_message_summary WHERE run_date >= date_sub(current_date(), 14)
GROUP BY campaign_name, egress_status ORDER BY pieces DESC"""),
    ("ds_run", "Run status by day", """
SELECT run_date, campaign_status, COUNT(*) AS campaigns, SUM(messages_final) AS pieces
FROM camp_run_history GROUP BY run_date, campaign_status ORDER BY run_date, campaign_status"""),
    ("ds_exceptions", "Failed, late, or pending sends", """
SELECT send_date, send_time_ct, campaign_name, priority_class, exception_state, pending_pieces, error_message
FROM camp_run_exceptions
WHERE exception_state IN ('FAILED', 'NOT_DELIVERED', 'LATE', 'AWAITING_APPROVAL', 'PENDING')
ORDER BY CASE exception_state WHEN 'FAILED' THEN 1 WHEN 'NOT_DELIVERED' THEN 2 WHEN 'LATE' THEN 3 ELSE 4 END, send_date, send_time_ct"""),
]


def qlines(sql):
    import re
    sql = re.sub(r"\bcamp_([a-z_]+)\b", PREFIX + r"\1", sql)
    lines = [l + "\n" for l in sql.strip("\n").split("\n")]
    lines[-1] = lines[-1].rstrip("\n")
    return lines


def counter(name, title, field, col, x, y, color=None):
    return {
        "widget": {
            "name": name,
            "queries": [{"name": "main_query", "query": {"datasetName": "ds_kpi", "fields": [
                {"name": field, "expression": f"SUM(`{field}`)"}], "disaggregated": False}}],
            "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": {
                "fieldName": field, "displayName": title,
                "format": {"type": "number-plain", "decimalPlaces": {"type": "exact", "places": 0}}}},
                "frame": {"showTitle": True, "title": title}},
        },
        "position": {"x": x, "y": y, "width": 4, "height": 3},
    }


def bar(name, ds, title, x_field, y_field, y_title, x_title, x, y, w, h, color_field=None, stacked=False,
        y_expr=None):
    fields = [{"name": x_field, "expression": f"`{x_field}`"}]
    y_name = f"sum({y_field})"
    fields.append({"name": y_name, "expression": f"SUM(`{y_field}`)"})
    encodings = {
        "x": {"fieldName": x_field, "scale": {"type": "categorical"}, "axis": {"title": x_title}},
        "y": {"fieldName": y_name, "scale": {"type": "quantitative"}, "axis": {"title": y_title}},
    }
    if color_field:
        fields.append({"name": color_field, "expression": f"`{color_field}`"})
        encodings["color"] = {"fieldName": color_field, "scale": {"type": "categorical"}}
    return {
        "widget": {
            "name": name,
            "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": fields, "disaggregated": False}}],
            "spec": {"version": 3, "widgetType": "bar", "encodings": encodings,
                     "frame": {"showTitle": True, "title": title},
                     "mark": {"colors": ["#FF3621", "#00A972", "#FFAB00", "#8BCAE7", "#AB4057", "#99DDB4", "#919191"]}},
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def table(name, ds, title, columns, x, y, w, h):
    fields = [{"name": c, "expression": f"`{c}`"} for c, _ in columns]
    return {
        "widget": {
            "name": name,
            "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": fields, "disaggregated": True}}],
            "spec": {"version": 2, "widgetType": "table", "frame": {"showTitle": True, "title": title},
                     "encodings": {"columns": [{"fieldName": c, "displayName": d} for c, d in columns]}},
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def text(name, md, x, y, w, h):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": md.split("\n")}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


layout = [
    text("h_title", "## Campaign Operations\nWhat goes out, when, how many pieces, and whether it ran. "
                    "Campaign-level only: no patient rows.", 0, 0, 12, 2),
    counter("k_sends_week", "Sends this week", "sends_this_week", None, 0, 2),
    counter("k_pieces_week", "Max pieces this week", "pieces_this_week", None, 4, 2),
    counter("k_reg_14d", "Regulatory notices, next 14 days", "regulatory_next_14d", None, 8, 2),
    counter("k_failed", "Failed sends", "failed_sends", None, 0, 5),
    counter("k_late", "Late or undelivered", "late_sends", None, 4, 5),
    text("k_note", "FAILED: a run failed. LATE: the send date passed with no run. NOT_DELIVERED: messages still "
                   "pending after their send window.", 8, 5, 4, 3),
    table("t_week", "ds_week", "This week's sends (Monday to Sunday)",
          [("send_date", "Date"), ("send_time_ct", "Time (CT)"), ("campaign_name", "Campaign"),
           ("priority_class", "Priority"), ("channel", "Channel"), ("owner_team", "Owner"),
           ("max_pieces", "Max pieces"), ("approved", "Approved")], 0, 8, 12, 8),
    bar("b_daily", "ds_daily", "Max pieces by day and priority (next 30 days)", "send_date", "pieces",
        "Pieces", "Send date", 0, 16, 12, 6, color_field="priority_class", stacked=True),
    bar("b_channel", "ds_channel", "Max pieces by channel (next 30 days)", "channel", "pieces",
        "Pieces", "Channel", 0, 22, 6, 6),
    bar("b_type", "ds_type", "Delivery status by campaign (last 14 days)", "campaign_name", "pieces",
        "Pieces", "Campaign", 6, 22, 6, 6, color_field="egress_status"),
    bar("b_run", "ds_run", "Campaign runs by day and status", "run_date", "campaigns",
        "Campaigns", "Run date", 0, 28, 6, 6, color_field="campaign_status"),
    table("t_exceptions", "ds_exceptions", "Failed, late, or pending sends",
          [("send_date", "Date"), ("send_time_ct", "Time (CT)"), ("campaign_name", "Campaign"),
           ("priority_class", "Priority"), ("exception_state", "State"), ("pending_pieces", "Undelivered"), ("error_message", "Error")],
          6, 28, 6, 6),
]

serialized = {
    "datasets": [{"name": n, "displayName": d, "queryLines": qlines(q)} for n, d, q in DATASETS],
    "pages": [{"name": "campaign_ops", "displayName": "Campaign Operations", "pageType": "PAGE_TYPE_CANVAS",
               "layoutVersion": "GRID_V1", "layout": layout}],
    "uiSettings": {"theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"}},
}

if __name__ == "__main__":
    os.makedirs(HERE, exist_ok=True)
    out = os.path.join(HERE, "campaign_operations.lvdash.json")
    with open(out, "w") as f:
        json.dump(serialized, f, indent=1)
    # sanity: grid bounds
    bad = [i["widget"]["name"] for i in layout if i["position"]["x"] + i["position"]["width"] > 12]
    print("widgets", len(layout), "out_of_bounds", bad)
