with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view explode(context) e as k, attr_value
),
parsed as (
    -- Higher-order array functions
    select
        id,
        service,
        level,
        latency_ms,
        status_code,
        k,
        attr_value,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 4) != 0 as int) as has_fault
    from lines
),
kept_keys as (
    -- Set difference on keys
    select service, id
    from parsed
    except all
    select service, id
    from app_logs
    where status_code <= 362
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from parsed s
    inner join kept_keys k on s.service = k.service and s.id = k.id
),
grouped as (
    -- Grouped metrics
    select
        k,
        case when status_code >= 435 then 'LARGE' else 'SMALL' end as status_code_tier,
        case when latency_ms >= 2568 then 'LARGE' else 'SMALL' end as latency_ms_tier,
        count(distinct level) as distinct_levels,
        approx_percentile(latency_ms, 0.9) as latency_ms_p90,
        sum(status_code_high) as total_status_code_high,
        count(*) filter (where k = 'version') as version_rows
    from reconciled
    group by cube (k, case when status_code >= 435 then 'LARGE' else 'SMALL' end, case when latency_ms >= 2568 then 'LARGE' else 'SMALL' end)
    having sum(status_code_high) >= 0
)
select
    k,
    status_code_tier,
    latency_ms_p90,
    total_status_code_high
from grouped
order by total_status_code_high desc nulls first
