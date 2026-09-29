with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view explode(context) li as attr_key, attr_value
),
unpacked as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        host,
        status_code,
        attr_key,
        attr_value,
        cast((status_code & 8) != 0 as int) as has_fault,
        shiftright(status_code, 2) as status_code_high
    from exploded
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat(attr_key, ', ', host) as attr_key_host,
        round(status_code * 1.15, 2) as status_code_scaled
    from unpacked
    where attr_key is not null
      and host in ('h3', 'h4')
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        host,
        status_code,
        attr_key,
        dense_rank() over w_part as status_code_dense_rank,
        row_number() over (partition by attr_key, host order by status_code_scaled desc, id, attr_key) as event_seq
    from base
    window w_part as (partition by host order by status_code desc)
),
latest as (
    -- QUALIFY-style window filter
    select *
    from with_windows
    qualify row_number() over (partition by host order by status_code_dense_rank desc nulls first, id, attr_key) = 1
),
grouped as (
    -- Grouped metrics
    select
        host,
        attr_key,
        case when event_seq >= 8 then 'LARGE' else 'SMALL' end as event_seq_tier,
        approx_percentile(status_code_dense_rank, 0.9) as status_code_dense_rank_p90,
        count(distinct attr_key) as distinct_attr_keys,
        sum(case when attr_key = 'region' then status_code_dense_rank else 0 end) as region_status_code_dense_rank,
        round(avg(status_code_dense_rank), 2) as avg_status_code_dense_rank,
        size(collect_set(host)) as num_hosts
    from latest
    group by grouping sets ((host, attr_key, case when event_seq >= 8 then 'LARGE' else 'SMALL' end), (host, attr_key), ())
)
select attr_key, status_code_dense_rank_p90
from grouped
order by status_code_dense_rank_p90 desc
