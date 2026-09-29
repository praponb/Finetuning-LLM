with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) x as k, attr_value
),
enriched as (
    -- Ranking and gap features
    select
        service,
        level,
        avg(status_code) over w_part as status_code_avg_30m,
        max(status_code) over w_part as status_code_max_30m,
        stddev_samp(status_code) over w_grp as service_stddev_samp_status_code
    from lines
    where k in ('version', 'region', 'trace')
    window w_part as (order by cast(log_ts as bigint) range between 1800 preceding and current row), w_grp as (partition by service, level)
),
metrics as (
    -- Window metrics over several different partitions
    select *, round(status_code_avg_30m / sum(status_code_avg_30m) over (partition by service), 4) as status_code_avg_30m_share
    from enriched
),
enriched_app_logs as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over (order by status_code_avg_30m nulls last) as status_code_avg_30m_dense_rank,
        dense_rank() over (partition by level order by status_code_max_30m nulls first) as status_code_max_30m_dense_rank,
        round(service_stddev_samp_status_code / sum(service_stddev_samp_status_code) over (partition by level, service), 4) as service_stddev_samp_status_code_share
    from metrics
),
stats as (
    -- Multi-level aggregation
    select
        level,
        sum(case when level = 'DEBUG' then status_code_max_30m else 0 end) as debug_status_code_max_30m,
        size(collect_set(service)) as num_services,
        count(case when level = 'DEBUG' then 1 end) as debug_count,
        grouping(level) as grouping_id
    from enriched_app_logs
    group by rollup (level)
    having sum(case when level = 'DEBUG' then status_code_max_30m else 0 end) < 1000000
)
select
    debug_status_code_max_30m,
    num_services,
    grouping_id
from stats;
