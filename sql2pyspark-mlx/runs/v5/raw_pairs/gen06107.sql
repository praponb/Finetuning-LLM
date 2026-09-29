with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        k,
        value
    from app_logs
    lateral view outer explode(context) e as k, value
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        host,
        k,
        value,
        round(status_code / latency_ms, 2) as status_code_per_latency_ms,
        coalesce(k, 'DIRECT') as resolved_k
    from exploded
    where level not in ('INFO', 'ERROR')
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        concat_ws('-', host, k) as host_k_key,
        trim(host) as clean_host,
        coalesce(k, 'OTHER') as resolved_k_2
    from filtered
    where k in ('region', 'trace')
),
kept_keys as (
    -- Set intersection on keys
    select host
    from valid
    intersect all
    select host
    from app_logs
    where log_ts >= '2023-04-05'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join kept_keys k on s.host = k.host
)
-- Grouped metrics
select
    case when status_code_per_latency_ms >= 78 then 'LARGE' else 'SMALL' end as status_code_per_latency_ms_tier,
    max(status_code_per_latency_ms) filter (where host = 'h4') as h4_max_status_code_per_latency_ms,
    count(case when host = 'h2' then 1 end) as h2_count,
    max(status_code_per_latency_ms) filter (where k = 'trace') as trace_max_status_code_per_latency_ms,
    sum(case when host = 'h1' then status_code_per_latency_ms else 0 end) as h1_status_code_per_latency_ms
from verified
group by case when status_code_per_latency_ms >= 78 then 'LARGE' else 'SMALL' end
having count(case when host = 'h2' then 1 end) >= 1
order by status_code_per_latency_ms_tier nulls last;
