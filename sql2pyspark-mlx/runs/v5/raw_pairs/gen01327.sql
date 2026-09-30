with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        latency_ms,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) t as key, attr_value
),
base as (
    -- Keep recent, valid rows
    select
        *,
        status_code - latency_ms as status_code_minus_latency_ms,
        upper(host) as host_upper,
        coalesce(key, 'DIRECT') as resolved_key
    from lines
    where key is not null
),
enriched as (
    -- Ranking and gap features
    select *, percent_rank() over (order by latency_ms desc) as latency_ms_percent_rank
    from base
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        host,
        latency_ms,
        status_code,
        key,
        attr_value,
        status_code_minus_latency_ms,
        latency_ms_percent_rank,
        round(latency_ms / status_code, 2) as latency_ms_per_status_code
    from enriched
    where latency_ms between 818 and 1053
),
matched_keys as (
    -- Set difference on keys
    select id
    from filtered
    except distinct
    select id
    from app_logs
    where status_code > 323
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    join matched_keys k on s.id = k.id
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        host,
        latency_ms,
        key,
        latency_ms_per_status_code,
        dense_rank() over (partition by key order by status_code) as status_code_dense_rank,
        dense_rank() over (partition by key order by status_code desc) as status_code_dense_rank_2,
        percent_rank() over w_running as latency_ms_percent_rank_percent_rank,
        corr(latency_ms, status_code) over (partition by host) as corr_latency_ms_status_code
    from kept_rows
    window w_running as (order by latency_ms_percent_rank desc nulls last)
)
-- Multi-level aggregation
select
    case when status_code_dense_rank_2 >= 6 then 'HIGH' else 'LOW' end as status_code_dense_rank_2_tier,
    key,
    round(sum(latency_ms_percent_rank_percent_rank) / nullif(count(case when host = 'h4' then 1 end), 0), 2) as latency_ms_percent_rank_percent_rank_per_h4,
    approx_percentile(status_code_dense_rank, array(0.25, 0.5, 0.75)) as status_code_dense_rank_quartiles,
    sum(status_code_dense_rank_2) as total_status_code_dense_rank_2,
    round(avg(corr_latency_ms_status_code), 2) as avg_corr_latency_ms_status_code,
    size(collect_set(key)) as num_keys
from with_windows
group by status_code_dense_rank_2_tier, key
