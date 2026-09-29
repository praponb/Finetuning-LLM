with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        v
    from app_logs
    lateral view outer explode(context) t as key, v
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        host,
        level,
        latency_ms,
        key,
        coalesce(key, 'N/A') key_filled,
        coalesce(key, 'DIRECT') key_filled_2
    from flattened
    where log_ts > date '2025-06-01'
      and level = 'ERROR'
),
matched_keys as (
    -- Set intersection on keys
    select id
    from base
    intersect all
    select id
    from app_logs
    where service not in ('checkout', 'search')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join matched_keys k on s.id = k.id
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        host,
        key,
        case when host = 'h3' then 1 else 0 end is_h3,
        initcap(trim(level)) level_title,
        coalesce(key, 'N/A') resolved_key
    from reconciled
    where latency_ms >= 725
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        host,
        key,
        is_h3,
        level_title,
        resolved_key,
        percent_rank() over w_hist is_h3_percent_rank,
        rank() over w_grp is_h3_rank
    from cleaned
    window w_hist as (order by is_h3 asc nulls first), w_grp as (order by is_h3 desc nulls last)
),
base_app_logs as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        is_h3_rank / nullif(is_h3_percent_rank, 0) is_h3_rank_per_is_h3_percent_rank,
        coalesce(key, 'N/A') key_clean
    from with_windows
    where key is not null
),
with_windows_app_logs as (
    -- Running and moving aggregates
    select
        id,
        host,
        key,
        is_h3,
        level_title,
        resolved_key,
        is_h3_percent_rank,
        is_h3_rank,
        is_h3_rank_per_is_h3_percent_rank,
        key_clean,
        sum(is_h3_rank) over w_part key_sum_is_h3_rank,
        percent_rank() over (order by is_h3 asc nulls last) is_h3_percent_rank_2
    from base_app_logs
    window w_part as (partition by key)
)
select
    id,
    host,
    key,
    is_h3,
    resolved_key,
    is_h3_percent_rank,
    is_h3_rank,
    is_h3_rank_per_is_h3_percent_rank,
    key_sum_is_h3_rank,
    case when host rlike '^(h1|h3)$' then 'EXTENDED' when host rlike '^(h2|h4)$' then 'PRIORITY' else 'GROUP_A' end as host_zone
from with_windows_app_logs
order by id desc
