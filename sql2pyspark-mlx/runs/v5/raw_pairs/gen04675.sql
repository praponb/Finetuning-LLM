with lines as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        log_ts,
        level,
        key,
        attr_value
    from app_logs
    lateral view explode(context) li as key, attr_value
),
deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        log_ts,
        key,
        attr_value
    from lines
    qualify row_number() over (partition by level, host order by log_ts desc, id asc, key asc) = 1
),
ranked as (
    -- Keep one row per key (latest first)
    select
        id,
        log_ts,
        key,
        attr_value
    from (
        select *, rank() over (partition by key order by log_ts asc) as latest_rank
        from deduped
    ) x
    where latest_rank = 1
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        datediff(current_date(), log_ts) as days_since_log_ts,
        coalesce(key, 'OTHER') as key_filled
    from ranked
    where to_date(log_ts) < '2023-04-17'
)
-- Window metrics over several different partitions
select
    id,
    key,
    days_since_log_ts,
    lead(days_since_log_ts, 2) over (order by log_ts asc, id asc, key asc) as next_days_since_log_ts,
    ntile(10) over (partition by key order by days_since_log_ts desc, id asc, key asc) as days_since_log_ts_bucket,
    ntile(5) over (partition by key order by days_since_log_ts asc, id asc, key asc) as days_since_log_ts_bucket_2
from cleaned
where key = 'version';
