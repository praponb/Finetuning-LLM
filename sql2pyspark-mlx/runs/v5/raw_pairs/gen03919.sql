select
    id,
    service,
    payload_json,
    context,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier as tier,
    get_json_object(payload_json, '$.user.id') as user_id,
    coalesce(context['version'], 'direct') as version_or_default,
    coalesce(context['trace'], 'none') as trace_or_default,
    get_json_object(payload_json, '$.user.tier') as user_tier
from app_logs;
