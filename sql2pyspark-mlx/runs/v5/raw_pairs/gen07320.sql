with lines as (
    select
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view outer explode(context) e as attr_key, attr_value
),
parsed as (
    select
        host,
        status_code,
        attr_key,
        attr_value,
        cast((status_code & 4) != 0 as int) as has_fault,
        shiftright(status_code, 2) as status_code_high
    from lines
),
metrics as (
    select
        *,
        min(has_fault) over (partition by attr_key) as attr_key_min_has_fault,
        cume_dist() over (order by status_code_high asc nulls first) as status_code_high_cume_dist,
        sum(has_fault) over (partition by attr_key, host) as attr_key_sum_has_fault
    from parsed
),
enriched as (
    select
        *,
        min(attr_key_sum_has_fault) over w_grp as host_min_attr_key_sum_has_fault,
        rank() over (partition by host order by has_fault asc nulls first) as has_fault_rank,
        dense_rank() over w_grp_2 as status_code_high_cume_dist_dense_rank
    from metrics
    window w_grp as (partition by host), w_grp_2 as (order by status_code_high_cume_dist desc)
)
select
    attr_key,
    case when status_code >= 419 then 'LARGE' else 'SMALL' end as status_code_tier,
    case when attr_key_min_has_fault >= 1 then 'PREMIUM' else 'STANDARD' end as attr_key_min_has_fault_tier,
    percentile_approx(has_fault, 0.95) as has_fault_p95,
    size(collect_set(host)) as num_hosts,
    avg(attr_key_sum_has_fault) as avg_attr_key_sum_has_fault,
    percentile_approx(attr_key_min_has_fault, array(0.25, 0.5, 0.75)) as attr_key_min_has_fault_quartiles
from enriched
group by attr_key, case when status_code >= 419 then 'LARGE' else 'SMALL' end, case when attr_key_min_has_fault >= 1 then 'PREMIUM' else 'STANDARD' end
order by attr_key desc
