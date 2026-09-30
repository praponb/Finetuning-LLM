with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view explode(context) li as k, attr_value
),
parsed as (
    -- Higher-order array functions
    select
        id,
        status_code,
        k,
        attr_value,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 4) != 0 as int) as has_fault
    from flattened
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        avg(status_code_high) over (partition by k order by status_code, id, k rows between 2 preceding and current row) as status_code_high_avg_2,
        sum(status_code_high) over (partition by k) as k_sum_status_code_high,
        ntile(3) over (partition by k order by status_code, id, k) as status_code_bucket
    from parsed
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        k,
        has_fault,
        k_sum_status_code_high,
        sum(has_fault) over w_running as last7_sum_has_fault,
        avg(has_fault) over w_running as last7_avg_has_fault,
        count(has_fault) over w_running as last7_count_has_fault,
        lag(status_code_bucket, 2) over (partition by k order by status_code_high_avg_2, id, k) as prev_status_code_bucket
    from metrics
    where has_fault between 0 and 1
    window w_running as (partition by k order by status_code_high, id, k rows between 6 preceding and current row)
)
-- Multi-level aggregation
select
    k,
    case when prev_status_code_bucket >= 7 then 'PREMIUM' else 'STANDARD' end as prev_status_code_bucket_tier,
    case when last7_avg_has_fault >= 0.87 then 'PREMIUM' else 'STANDARD' end as last7_avg_has_fault_tier,
    round(sum(last7_count_has_fault) / nullif(count(case when k = 'trace' then 1 end), 0), 2) as last7_count_has_fault_per_trace,
    sum(last7_count_has_fault) as total_last7_count_has_fault,
    sort_array(collect_set(k)) as k_list,
    approx_percentile(prev_status_code_bucket, array(0.25, 0.5, 0.75)) as prev_status_code_bucket_quartiles,
    approx_percentile(k_sum_status_code_high, array(0.25, 0.5, 0.75)) as k_sum_status_code_high_quartiles,
    max(k_sum_status_code_high) as max_k_sum_status_code_high,
    grouping(k) as grouping_level
from enriched
group by grouping sets ((k, case when prev_status_code_bucket >= 7 then 'PREMIUM' else 'STANDARD' end, case when last7_avg_has_fault >= 0.87 then 'PREMIUM' else 'STANDARD' end), (k, case when prev_status_code_bucket >= 7 then 'PREMIUM' else 'STANDARD' end), (k))
order by last7_avg_has_fault_tier
