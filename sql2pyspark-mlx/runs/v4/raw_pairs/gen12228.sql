with flattened_attrs as (
    -- Semi-structured parsing
    select
        id,
        service,
        log_ts,
        shiftright(status_code, 2) as status_code_high,
        array_contains(map_keys(context), 'version') as has_version,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier as tier,
        cast((status_code & 2) != 0 as int) as is_flagged
    from app_logs
    where service in ('billing', 'checkout', 'search')
)
-- Ranking and gap features
select
    id,
    service,
    log_ts,
    status_code_high,
    has_version,
    tier,
    is_flagged,
    sum(status_code_high) over (partition by service order by cast(log_ts as bigint) range between 7200 preceding and current row) as status_code_high_sum_2h,
    row_number() over (partition by service order by status_code_high desc, id) as row_seq
from flattened_attrs;
