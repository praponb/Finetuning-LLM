with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        level,
        latency_ms,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) t as key, attr_value
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        substring(key, 1, 2) key_prefix,
        coalesce(key, 'UNKNOWN') key_clean
    from flattened
    where key in ('region', 'version')
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when key in ('version', 'region') then 'Tier 1' else 'Other' end key_group,
        coalesce(key, 'UNKNOWN') resolved_key
    from filtered
    where key is not null
      and service = 'billing'
),
filtered_app_logs as (
    -- Basic cleansing and defaults
    select *, upper(level) level_upper
    from cleaned
    where key is not null
),
cleaned_app_logs as (
    -- Keep recent, valid rows
    select
        id,
        service,
        level,
        latency_ms,
        key,
        key_prefix,
        key_clean,
        key_group,
        resolved_key,
        level_upper,
        concat(service, ' ', key) service_key
    from filtered_app_logs
    where key is not null
      and service = 'checkout'
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        max(latency_ms) over (partition by service) service_max_latency_ms,
        cume_dist() over (order by latency_ms nulls first) latency_ms_cume_dist,
        round(latency_ms / sum(latency_ms) over (partition by service), 4) latency_ms_share
    from cleaned_app_logs
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        rank() over w_running service_max_latency_ms_rank,
        rank() over (partition by service order by latency_ms_cume_dist nulls first) latency_ms_cume_dist_rank
    from with_windows
    window w_running as (partition by key order by service_max_latency_ms nulls last)
)
select
    level,
    key_prefix,
    key_clean,
    key_group,
    resolved_key,
    level_upper,
    service_key,
    service_max_latency_ms,
    latency_ms_share,
    case when level rlike '^(DEBUG|WARN)$' then 'CORE' when level rlike '^(INFO|ERROR)$' then 'OTHER_ZONE' else 'EXTENDED' end as level_zone
from scored
order by level_zone desc;
