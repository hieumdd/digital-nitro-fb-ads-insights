from facebook.pipeline.interface import AdsInsights


campaign_insights = AdsInsights(
    "CampaignInsights",
    "campaign",
    [
        "date_start",
        "date_stop",
        "account_id",
        "campaign_id",
        "campaign_name",
        "clicks",
        "inline_link_clicks",
        "spend",
        "impressions",
        "actions",
        "action_values",
    ],
    lambda rows: [
        {
            "account_id": row["account_id"],
            "date_start": row["date_start"],
            "date_stop": row["date_stop"],
            "campaign_id": row["campaign_id"],
            "campaign_name": row["campaign_name"],
            "clicks": row.get("clicks"),
            "inline_link_clicks": row.get("inline_link_clicks"),
            "spend": row.get("spend"),
            "impressions": row.get("impressions"),
            "actions": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["actions"]
            ]
            if row.get("actions")
            else [],
            "action_values": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["action_values"]
            ]
            if row.get("action_values")
            else [],
        }
        for row in rows
    ],
    [
        {"name": "account_id", "type": "NUMERIC"},
        {"name": "date_start", "type": "DATE"},
        {"name": "date_stop", "type": "DATE"},
        {"name": "campaign_id", "type": "NUMERIC"},
        {"name": "campaign_name", "type": "STRING"},
        {"name": "clicks", "type": "NUMERIC"},
        {"name": "inline_link_clicks", "type": "NUMERIC"},
        {"name": "spend", "type": "NUMERIC"},
        {"name": "impressions", "type": "NUMERIC"},
        {
            "name": "actions",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "action_type", "type": "STRING"},
                {"name": "value", "type": "NUMERIC"},
                {"name": "_1d_view", "type": "NUMERIC"},
                {"name": "_1d_click", "type": "NUMERIC"},
                {"name": "_7d_click", "type": "NUMERIC"},
                {"name": "_7d_view", "type": "NUMERIC"},
            ],
        },
        {
            "name": "action_values",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "action_type", "type": "STRING"},
                {"name": "value", "type": "NUMERIC"},
                {"name": "_1d_view", "type": "NUMERIC"},
                {"name": "_1d_click", "type": "NUMERIC"},
                {"name": "_7d_click", "type": "NUMERIC"},
                {"name": "_7d_view", "type": "NUMERIC"},
            ],
        },
    ],
    [
        "date_start",
        "date_stop",
        "account_id",
        "campaign_id",
    ],
)
