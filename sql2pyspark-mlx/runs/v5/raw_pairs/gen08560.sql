with lines as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        level,
        latency_ms,
        status_code,
        key,
        value
    from app_logs
    lateral view outer explode(context) x as key, value
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        lower(key) as key_lower,
        lower(level) as level_lower,
        coalesce(key, 'DIRECT') as key_filled
    from lines
    where host in ('h2', 'h4')
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        latency_ms * status_code as latency_ms_times_status_code,
        concat(host, ' - ', key) as host_key
    from base
    where host != 'h3'
),
cleaned_app_logs as (
    -- Basic cleansing and defaults
    select
        *,
        trim(host) as clean_host,
        substring(host, 1, 2) as host_prefix,
        coalesce(key, 'UNKNOWN') as key_filled_2
    from cleaned
    where key is not null
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        host,
        level,
        latency_ms,
        status_code,
        key,
        key_lower,
        level_lower,
        key_filled,
        host_key,
        host_prefix,
        key_filled_2,
        coalesce(key, 'Unknown') as key_filled_3
    from cleaned_app_logs
    where key is distinct from 'trace'
),
base_app_logs as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        cast(latency_ms as string) as latency_ms_str,
        coalesce(key, 'UNKNOWN') as resolved_key
    from filtered
    where key is not null
      and key is distinct from 'region'
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        round(latency_ms / sum(latency_ms) over (partition by key), 4) as latency_ms_share,
        corr(latency_ms, status_code) over (partition by key) as corr_latency_ms_status_code,
        rank() over w_grp as status_code_rank,
        corr(status_code, latency_ms) over w_part as corr_status_code_latency_ms
    from base_app_logs
    window w_grp as (order by status_code desc), w_part as (partition by host, key)
)
select
    id,
    level,
    status_code,
    key,
    key_lower,
    level_lower,
    key_filled,
    host_key,
    key_filled_2,
    case when key rlike '^(trace|version)$' then 'OTHER_ZONE' else 'GROUP_B' end as key_zone
from with_windows
order by key desc nulls last
