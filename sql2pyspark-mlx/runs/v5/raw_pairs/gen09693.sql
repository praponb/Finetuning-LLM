with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        level,
        status_code,
        k,
        value
    from app_logs
    lateral view explode(context) t as k, value
),
unpacked as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        log_ts,
        k,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 2) != 0 as int) as has_fault
    from exploded
    where k in ('region', 'version')
),
remaining_keys as (
    -- Set difference on keys
    select id
    from unpacked
    except distinct
    select id
    from app_logs
    where latency_ms > 1170
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from unpacked s
    join remaining_keys k on s.id = k.id
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, quarter(log_ts) log_ts_quarter
    from verified
    where k is not null
      and log_ts < '2024-05-19'
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        to_date(log_ts) log_ts_date,
        has_fault - status_code_high has_fault_minus_status_code_high
    from cleaned
    where log_ts >= date '2025-06-01'
),
metrics as (
    -- Ranking and gap features
    select
        id,
        k,
        status_code_high,
        has_fault,
        log_ts_quarter,
        rank() over (partition by k order by status_code_high desc nulls last) status_code_high_rank,
        rank() over (partition by k order by has_fault nulls first) has_fault_rank,
        rank() over (partition by k order by has_fault nulls last) has_fault_rank_2,
        dense_rank() over w_ordered has_fault_minus_status_code_high_dense_rank,
        avg(has_fault_minus_status_code_high) over (partition by k order by cast(log_ts as bigint) range between 1800 preceding and current row) has_fault_minus_status_code_high_avg_30m
    from valid
    window w_ordered as (order by has_fault_minus_status_code_high nulls last)
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        status_code_high,
        has_fault,
        log_ts_quarter,
        has_fault_rank,
        has_fault_minus_status_code_high_dense_rank,
        has_fault_minus_status_code_high_avg_30m,
        rank() over (partition by k order by log_ts_quarter desc nulls first) log_ts_quarter_rank,
        round(status_code_high_rank / sum(status_code_high_rank) over (partition by k), 4) status_code_high_rank_share,
        stddev_samp(has_fault_minus_status_code_high_dense_rank) over (partition by k) k_stddev_samp_has_fault_minus_status_code_high_dense_rank,
        cume_dist() over w_recent status_code_high_cume_dist
    from metrics
    window w_recent as (partition by k order by status_code_high desc nulls first)
),
grouped as (
    -- Aggregate with conditional metrics
    select
        case when status_code_high_rank_share >= 0.7 then 'LARGE' else 'SMALL' end as status_code_high_rank_share_tier,
        min(status_code_high_cume_dist) as min_status_code_high_cume_dist,
        sum(has_fault_minus_status_code_high_avg_30m) as total_has_fault_minus_status_code_high_avg_30m,
        percentile_approx(has_fault_minus_status_code_high_avg_30m, array(0.25, 0.5, 0.75)) as has_fault_minus_status_code_high_avg_30m_quartiles
    from enriched
    group by 1
)
select
    status_code_high_rank_share_tier,
    min_status_code_high_cume_dist,
    total_has_fault_minus_status_code_high_avg_30m,
    has_fault_minus_status_code_high_avg_30m_quartiles
from grouped
order by total_has_fault_minus_status_code_high_avg_30m nulls first, min_status_code_high_cume_dist;
