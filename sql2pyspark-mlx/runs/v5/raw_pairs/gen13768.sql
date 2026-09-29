with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) x as k, attr_value
),
metrics as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by k order by status_code) as status_code_dense_rank,
        percent_rank() over (partition by k order by latency_ms nulls last) as latency_ms_percent_rank
    from flattened
    where latency_ms = 765
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        status_code * latency_ms_percent_rank as status_code_times_latency_ms_percent_rank,
        status_code_dense_rank * status_code as status_code_dense_rank_times_status_code
    from metrics
    where k is not null
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        level,
        latency_ms,
        status_code,
        k,
        attr_value,
        status_code_dense_rank,
        latency_ms_percent_rank,
        status_code_times_latency_ms_percent_rank,
        status_code_dense_rank_times_status_code,
        coalesce(status_code_times_latency_ms_percent_rank, 0) as status_code_times_latency_ms_percent_rank_filled,
        coalesce(k, 'N/A') as resolved_k
    from filtered
    where k is not null
),
daily as (
    -- Multi-level aggregation
    select
        case when status_code >= 417 then 'HIGH' else 'LOW' end as status_code_tier,
        case when latency_ms_percent_rank >= 0.76 then 'PREMIUM' else 'STANDARD' end as latency_ms_percent_rank_tier,
        case when status_code_times_latency_ms_percent_rank >= 77 then 'LARGE' else 'SMALL' end as status_code_times_latency_ms_percent_rank_tier,
        bit_and(latency_ms) as latency_ms_bit_and,
        count(case when level = 'ERROR' then 1 end) as error_count,
        count(distinct level) as distinct_levels,
        grouping(case when status_code >= 417 then 'HIGH' else 'LOW' end) as grouping_id
    from cleaned
    group by rollup (case when status_code >= 417 then 'HIGH' else 'LOW' end, case when latency_ms_percent_rank >= 0.76 then 'PREMIUM' else 'STANDARD' end, case when status_code_times_latency_ms_percent_rank >= 77 then 'LARGE' else 'SMALL' end)
)
select
    status_code_tier,
    latency_ms_percent_rank_tier,
    status_code_times_latency_ms_percent_rank_tier,
    latency_ms_bit_and,
    error_count,
    distinct_levels,
    grouping_id
from daily
order by error_count desc nulls first, distinct_levels desc;
