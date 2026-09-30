with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        level,
        status_code,
        key,
        value
    from app_logs
    lateral view outer explode(context) li as key, value
),
flattened_attrs as (
    -- Semi-structured parsing
    select
        service,
        host,
        level,
        value,
        cast((status_code & 8) != 0 as int) as flag_bit8,
        shiftright(status_code, 2) as status_code_high
    from lines
    where host in ('h1', 'h4')
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        avg(status_code_high) over (partition by level) as level_avg_status_code_high,
        avg(status_code_high) over (partition by host) as host_avg_status_code_high,
        cume_dist() over (partition by host order by status_code_high) as status_code_high_cume_dist,
        percent_rank() over (partition by service order by status_code_high desc nulls first) as status_code_high_percent_rank,
        percent_rank() over (partition by service order by flag_bit8 desc nulls last) as flag_bit8_percent_rank
    from flattened_attrs
    where service = 'checkout'
),
cleaned as (
    -- Basic cleansing and defaults
    select
        service,
        host,
        level,
        value,
        status_code_high,
        level_avg_status_code_high,
        host_avg_status_code_high,
        status_code_high_cume_dist,
        status_code_high_percent_rank,
        flag_bit8_percent_rank,
        status_code_high / nullif(status_code_high_cume_dist, 0) as status_code_high_per_status_code_high_cume_dist
    from enriched
    where status_code_high <= 2
),
remaining_keys as (
    -- Set difference on keys
    select service, level
    from cleaned
    except
    select service, level
    from app_logs
    where latency_ms > 1191
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join remaining_keys k on s.service = k.service and s.level = k.level
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        sum(status_code_high_cume_dist) over w_recent as service_sum_status_code_high_cume_dist,
        min(status_code_high_per_status_code_high_cume_dist) over (partition by level) as level_min_status_code_high_per_status_code_high_cume_dist,
        percent_rank() over w_part as status_code_high_percent_rank_percent_rank
    from reconciled
    window w_recent as (partition by service), w_part as (partition by service, level order by status_code_high_percent_rank desc nulls last)
)
-- Grouped metrics
select
    case when level_avg_status_code_high >= 2 then 'PREMIUM' else 'STANDARD' end as level_avg_status_code_high_tier,
    case when service_sum_status_code_high_cume_dist >= 4 then 'LARGE' else 'SMALL' end as service_sum_status_code_high_cume_dist_tier,
    service,
    count_if(level = 'ERROR') as error_events,
    round(avg(service_sum_status_code_high_cume_dist), 2) as avg_service_sum_status_code_high_cume_dist,
    round(avg(service_sum_status_code_high_cume_dist), 2) as avg_service_sum_status_code_high_cume_dist_2,
    count(distinct host) as distinct_hosts,
    grouping(case when level_avg_status_code_high >= 2 then 'PREMIUM' else 'STANDARD' end) as grouping_level
from with_windows
group by grouping sets ((case when level_avg_status_code_high >= 2 then 'PREMIUM' else 'STANDARD' end, case when service_sum_status_code_high_cume_dist >= 4 then 'LARGE' else 'SMALL' end, service), (case when level_avg_status_code_high >= 2 then 'PREMIUM' else 'STANDARD' end, case when service_sum_status_code_high_cume_dist >= 4 then 'LARGE' else 'SMALL' end), (case when level_avg_status_code_high >= 2 then 'PREMIUM' else 'STANDARD' end))
having round(avg(service_sum_status_code_high_cume_dist), 2) < 1000000
