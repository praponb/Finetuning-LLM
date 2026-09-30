with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view explode(context) t as k, attr_value
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        host,
        level,
        latency_ms,
        status_code,
        k,
        attr_value,
        cast(status_code as double) status_code_str
    from exploded
    where k not in ('trace', 'version')
      and log_ts > to_timestamp('2025-06-01 16:00:00')
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        host,
        level,
        status_code,
        k,
        attr_value,
        status_code_str,
        concat(k, ' ', level) k_level,
        coalesce(k, 'DIRECT') resolved_k
    from base
    where k = 'trace'
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by level, k order by status_code, id, k) as rank_in_group
    from cleaned
    qualify rank_in_group = 1
),
enriched as (
    -- Ranking and gap features
    select
        *,
        ntile(4) over (partition by k order by status_code_str, id, k) status_code_str_bucket,
        row_number() over (order by status_code_str desc, id, k) row_seq
    from deduped
),
base_app_logs as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        level,
        status_code,
        k,
        k_level,
        resolved_k,
        rank_in_group,
        status_code_str_bucket,
        row_seq,
        cast(status_code_str_bucket as double) status_code_str_bucket_str,
        coalesce(k, 'OTHER') k_clean
    from enriched
    where status_code_str > 45
)
-- Multi-level aggregation
select
    level,
    size(collect_set(k)) as num_ks,
    round(sum(row_seq) / nullif(count(case when k = 'version' then 1 end), 0), 2) as row_seq_per_version
from base_app_logs
group by level
order by row_seq_per_version
