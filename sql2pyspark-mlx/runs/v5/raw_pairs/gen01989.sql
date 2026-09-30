with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        level,
        latency_ms,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) t as k, attr_value
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        k,
        attr_value,
        round(latency_ms / sum(latency_ms) over (partition by k), 4) as latency_ms_share,
        round(latency_ms / sum(latency_ms) over (partition by k), 4) as latency_ms_share_2,
        count(latency_ms) over (partition by level, k) as level_count_latency_ms,
        rank() over (order by latency_ms desc nulls first) as latency_ms_rank,
        cume_dist() over (order by latency_ms) as latency_ms_cume_dist
    from exploded
    where k = 'trace'
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        lower(k) as k_lower,
        case when k = 'region' then 1 else 0 end as is_region,
        coalesce(k, 'N/A') as resolved_k
    from metrics
    where k not in ('version', 'region')
),
cleaned_app_logs as (
    -- Basic cleansing and defaults
    select
        id,
        attr_value,
        latency_ms_share,
        latency_ms_share_2,
        level_count_latency_ms,
        latency_ms_rank,
        latency_ms_cume_dist,
        k_lower,
        latency_ms_rank / nullif(is_region, 0) as latency_ms_rank_per_is_region,
        upper(k) as k_upper,
        coalesce(k, 'UNKNOWN') as k_filled
    from cleaned
    where latency_ms_share between 0.31 and 0.76
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        attr_value,
        latency_ms_share,
        level_count_latency_ms,
        latency_ms_rank,
        k_lower,
        latency_ms_rank_per_is_region,
        k_filled,
        level_count_latency_ms * latency_ms_cume_dist as level_count_latency_ms_times_latency_ms_cume_dist,
        round(level_count_latency_ms * 1.1, 2) as adjusted_level_count_latency_ms
    from cleaned_app_logs
    where latency_ms_rank_per_is_region < 63
)
-- Grouped metrics
select
    case when level_count_latency_ms >= 18 then 'HIGH' else 'LOW' end as level_count_latency_ms_tier,
    percentile_approx(level_count_latency_ms, array(0.25, 0.5, 0.75)) as level_count_latency_ms_quartiles,
    approx_percentile(level_count_latency_ms, 0.5) as level_count_latency_ms_p50
from filtered
group by case when level_count_latency_ms >= 18 then 'HIGH' else 'LOW' end;
