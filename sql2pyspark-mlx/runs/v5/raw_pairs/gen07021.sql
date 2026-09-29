select
    id,
    service,
    log_ts,
    level,
    status_code,
    payload_json,
    context,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier as tier,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec,
    cast((status_code & 2) != 0 as int) as flag_bit2,
    coalesce(context['version'], 'unknown') as version_or_default
from app_logs;
