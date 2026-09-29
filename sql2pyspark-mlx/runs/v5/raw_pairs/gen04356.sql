with parsed as (
    -- Higher-order array functions
    select
        id,
        log_ts,
        latency_ms,
        status_code,
        payload_json,
        context,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier as tier,
        get_json_object(payload_json, '$.duration') as duration_text,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec,
        cast((status_code & 4) != 0 as int) as flag_bit4,
        get_json_object(payload_json, '$.user.id') as user_id
    from app_logs
    where status_code between 366 and 438
)
-- Multi-level aggregation
select
    tier,
    count(distinct tier) as distinct_tiers,
    count(distinct tier) as distinct_tiers_2
from parsed
group by tier
having distinct_tiers_2 >= 1;
