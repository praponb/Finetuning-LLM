with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        latency_ms,
        k,
        v
    from app_logs
    lateral view explode(context) e as k, v
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by service, host order by latency_ms desc nulls first, id, k) as row_num
        from lines
    ) x
    where row_num = 1
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from latest
    qualify row_number() over (partition by k order by latency_ms desc nulls last, id, k) = 1
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        k,
        row_num,
        count(latency_ms) over (partition by service order by row_num, id, k rows between 2 preceding and current row) as last3_count_latency_ms,
        min(latency_ms) over (partition by service order by row_num, id, k rows between 2 preceding and current row) as last3_min_latency_ms,
        avg(latency_ms) over (partition by service order by row_num, id, k rows between 2 preceding and current row) as last3_avg_latency_ms
    from deduped
    where k = 'trace'
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        k,
        row_num,
        last3_count_latency_ms,
        last3_avg_latency_ms,
        trim(k) as clean_k,
        length(k) as k_length,
        coalesce(k, 'NONE') as k_filled
    from with_windows
    where k is not null
      and k is distinct from 'version'
)
-- Aggregate with conditional metrics
select
    k,
    case when k_length >= 80 then 'HIGH' else 'LOW' end as k_length_tier,
    avg(last3_count_latency_ms) as avg_last3_count_latency_ms,
    size(collect_set(k)) as num_ks,
    size(collect_set(k)) as num_ks_2,
    count(case when k = 'region' then 1 end) as region_count,
    percentile_approx(last3_avg_latency_ms, 0.5) as last3_avg_latency_ms_p50,
    grouping_id() as grouping_id
from filtered
group by rollup (k, case when k_length >= 80 then 'HIGH' else 'LOW' end)
order by k_length_tier desc nulls last
