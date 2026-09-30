with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view explode(context) li as key, attr_value
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        level,
        key,
        lag(status_code) over (partition by level order by status_code, id, key) as prev_status_code,
        lead(status_code) over (partition by level order by status_code, id, key) as next_status_code,
        lead(latency_ms) over (partition by level order by status_code, id, key) as next_latency_ms,
        row_number() over (partition by level order by status_code, id, key) as event_seq,
        min(status_code) over (partition by key) as key_min_status_code
    from exploded
),
ranked as (
    -- QUALIFY-style window filter
    select
        id,
        key,
        prev_status_code,
        event_seq,
        key_min_status_code
    from with_windows
    qualify row_number() over (partition by key, level order by next_status_code desc, id, key) = 1
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        key,
        prev_status_code,
        event_seq,
        key_min_status_code,
        substring(key, 1, 3) as key_prefix
    from ranked
    where prev_status_code <= 277
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        key,
        event_seq,
        key_min_status_code,
        key_prefix,
        upper(key) as key_upper,
        case when key = 'version' then 1 else 0 end as is_version,
        coalesce(key, 'UNKNOWN') as key_filled
    from cleaned
    where key is not null
      and key is distinct from 'trace'
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        key,
        event_seq,
        key_prefix,
        key_filled
    from filtered
    qualify rank() over (partition by key order by event_seq nulls first) = 1
),
deduped_app_logs as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by key order by event_seq desc, id, key) as row_num
        from deduped
    ) x
    where row_num = 1
),
summary as (
    -- Aggregate with conditional metrics
    select
        case when event_seq >= 8 then 'PREMIUM' else 'STANDARD' end as event_seq_tier,
        count(distinct key) as distinct_keys,
        round(sum(event_seq) / nullif(count(case when key = 'region' then 1 end), 0), 2) as event_seq_per_region,
        count_if(key = 'region') as region_events
    from deduped_app_logs
    group by case when event_seq >= 8 then 'PREMIUM' else 'STANDARD' end
)
select event_seq_tier, region_events
from summary;
