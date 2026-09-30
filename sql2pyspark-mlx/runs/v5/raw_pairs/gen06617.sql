with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        latency_ms,
        status_code,
        attr_key,
        v
    from app_logs
    lateral view explode(context) x as attr_key, v
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by host, service order by status_code desc, id asc, attr_key asc) as row_num
        from exploded
    ) ranked
    where row_num = 1
),
kept_keys as (
    -- Set difference on keys
    select host, service
    from deduped
    except all
    select host, service
    from app_logs
    where to_date(log_ts) < '2023-07-01'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from deduped s
    join kept_keys k on s.host = k.host and s.service = k.service
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        host,
        latency_ms,
        attr_key,
        row_num,
        round(status_code, 1) as status_code_rounded
    from kept_rows
    where attr_key is not null
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        stddev_samp(status_code_rounded) over (partition by host) as host_stddev_samp_status_code_rounded,
        rank() over (partition by attr_key, host order by status_code_rounded desc nulls last) as status_code_rounded_rank,
        avg(latency_ms) over (partition by attr_key) as attr_key_avg_latency_ms
    from base
)
select
    id,
    host,
    latency_ms,
    attr_key,
    row_num,
    status_code_rounded,
    host_stddev_samp_status_code_rounded,
    status_code_rounded_rank,
    attr_key_avg_latency_ms
from enriched
order by status_code_rounded asc;
