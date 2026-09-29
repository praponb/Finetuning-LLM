with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        log_ts,
        latency_ms,
        status_code,
        key,
        value
    from app_logs
    lateral view outer explode(context) li as key, value
),
unpacked as (
    -- Higher-order array functions
    select
        id,
        latency_ms,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 8) != 0 as int) as flag_bit8
    from flattened
    where latency_ms < 1328
),
scored as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (order by flag_bit8 desc nulls last) as flag_bit8_percent_rank,
        rank() over (order by latency_ms desc nulls first) as latency_ms_rank
    from unpacked
    where flag_bit8 != 1
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        latency_ms,
        status_code_high,
        flag_bit8_percent_rank,
        latency_ms_rank,
        cume_dist() over (order by flag_bit8 nulls last) as flag_bit8_cume_dist,
        cume_dist() over (order by flag_bit8_percent_rank desc) as flag_bit8_percent_rank_cume_dist
    from scored
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        cume_dist() over (order by status_code_high desc nulls first) as status_code_high_cume_dist,
        percent_rank() over w_grp as latency_ms_percent_rank,
        cume_dist() over w_hist as flag_bit8_percent_rank_cume_dist_cume_dist,
        cume_dist() over (order by flag_bit8_cume_dist nulls first) as flag_bit8_cume_dist_cume_dist,
        rank() over (order by status_code_high nulls last) as status_code_high_rank
    from enriched
    window w_grp as (order by latency_ms nulls last), w_hist as (order by flag_bit8_percent_rank_cume_dist desc)
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        latency_ms,
        status_code_high,
        flag_bit8_percent_rank,
        latency_ms_rank,
        status_code_high_cume_dist,
        latency_ms_percent_rank,
        flag_bit8_cume_dist_cume_dist,
        status_code_high_rank,
        ceil(flag_bit8_percent_rank_cume_dist) as flag_bit8_percent_rank_cume_dist_whole
    from with_windows
    where flag_bit8_percent_rank_cume_dist_cume_dist > 0.79
),
with_windows_app_logs as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (order by flag_bit8_cume_dist_cume_dist desc) as flag_bit8_cume_dist_cume_dist_cume_dist,
        dense_rank() over (order by status_code_high_cume_dist nulls first) as status_code_high_cume_dist_dense_rank,
        rank() over (order by flag_bit8_percent_rank_cume_dist_whole desc) as flag_bit8_percent_rank_cume_dist_whole_rank,
        cume_dist() over (order by flag_bit8_cume_dist_cume_dist nulls first) as flag_bit8_cume_dist_cume_dist_cume_dist_2,
        cume_dist() over (order by latency_ms nulls last) as latency_ms_cume_dist
    from cleaned
)
-- Multi-level aggregation
select
    case when latency_ms_cume_dist >= 0.71 then 'PREMIUM' else 'STANDARD' end as latency_ms_cume_dist_tier,
    percentile_approx(flag_bit8_percent_rank_cume_dist_whole_rank, 0.9) as flag_bit8_percent_rank_cume_dist_whole_rank_p90,
    percentile_approx(flag_bit8_percent_rank_cume_dist_whole_rank, array(0.25, 0.5, 0.75)) as flag_bit8_percent_rank_cume_dist_whole_rank_quartiles,
    percentile_approx(latency_ms_percent_rank, 0.5) as latency_ms_percent_rank_p50,
    count(distinct id) as distinct_ids,
    bit_or(latency_ms) as latency_ms_bit_or,
    count(*) as event_count
from with_windows_app_logs
group by case when latency_ms_cume_dist >= 0.71 then 'PREMIUM' else 'STANDARD' end
having latency_ms_percent_rank_p50 < 1000000
