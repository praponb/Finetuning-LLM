select
    id,
    log_ts,
    context,
    get_json_object(payload_json, '$.duration') as duration_text,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier as tier
from app_logs;
