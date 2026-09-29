with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        v
    from app_logs
    lateral view explode(context) li as key, v
),
parsed as (
    -- Higher-order array functions
    select
        id,
        service,
        host,
        status_code,
        key,
        v,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 1) != 0 as int) as has_fault
    from exploded
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        length(host) as host_length,
        case when status_code_high >= 2 then 'High' when status_code_high >= 1 then 'Medium' else 'Low' end as status_code_high_tier,
        coalesce(key, 'OTHER') as key_filled
    from parsed
    where host = 'h1'
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from valid
    qualify row_number() over (partition by host order by status_code_high desc nulls last, id, key) = 1
),
matched_keys as (
    -- Set intersection on keys
    select id
    from deduped
    intersect all
    select id
    from app_logs
    where to_date(log_ts) < '2025-04-01'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from deduped s
    join matched_keys k on s.id = k.id
)
select
    id,
    service,
    host,
    status_code,
    key,
    v,
    host_length,
    status_code_high_tier,
    key_filled,
    case when service rlike '^(search|checkout)$' then 'EXTENDED' else 'OTHER_ZONE' end as service_zone
from reconciled
order by service_zone desc nulls first
