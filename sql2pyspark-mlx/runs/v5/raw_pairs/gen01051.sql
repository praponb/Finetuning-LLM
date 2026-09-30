with exploded as (
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
flattened_attrs as (
    -- Higher-order array functions
    select
        id,
        level,
        status_code,
        k,
        cast((status_code & 8) != 0 as int) as flag_bit8,
        shiftright(status_code, 2) as status_code_high
    from exploded
),
matched_keys as (
    -- Set difference on keys
    select level
    from flattened_attrs
    except
    select level
    from app_logs
    where status_code between 293 and 360
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from flattened_attrs s
    join matched_keys k on s.level = k.level
),
metrics as (
    -- Window metrics over several different partitions
    select *, dense_rank() over (partition by k, level order by status_code_high desc nulls first) as status_code_high_dense_rank
    from verified
    where flag_bit8 < 0
),
scored as (
    -- Running and moving aggregates
    select
        *,
        sum(flag_bit8) over (partition by k) as k_sum_flag_bit8,
        cume_dist() over (partition by k order by status_code) as status_code_cume_dist
    from metrics
),
base as (
    -- Basic cleansing and defaults
    select
        status_code,
        k,
        status_code_high,
        status_code_high_dense_rank,
        k_sum_flag_bit8,
        length(level) as level_length
    from scored
    where flag_bit8 <= 0
)
-- Multi-level aggregation
select
    k,
    round(sum(k_sum_flag_bit8) / nullif(count(case when k = 'version' then 1 end), 0), 2) as k_sum_flag_bit8_per_version,
    round(avg(status_code), 2) as avg_status_code
from base
group by k
having round(sum(k_sum_flag_bit8) / nullif(count(case when k = 'version' then 1 end), 0), 2) < 1000000;
