with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) t as k, attr_value
),
flattened_attrs as (
    -- Higher-order array functions
    select
        id,
        host,
        log_ts,
        status_code,
        k,
        attr_value,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 8) != 0 as int) as flag_bit8
    from lines
),
metrics as (
    -- Ranking and gap features
    select
        log_ts,
        k,
        attr_value,
        status_code_high,
        min(flag_bit8) over (partition by k) as k_min_flag_bit8,
        percent_rank() over w_recent as status_code_high_percent_rank
    from flattened_attrs
    where status_code_high >= 1
    window w_recent as (partition by k order by status_code_high)
),
scored as (
    -- Ranking and gap features
    select
        log_ts,
        k,
        attr_value,
        k_min_flag_bit8,
        sum(status_code_high) over (partition by k) as k_sum_status_code_high,
        cume_dist() over (partition by k order by status_code_high desc nulls last) as status_code_high_cume_dist
    from metrics
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, status_code_high_cume_dist / nullif(k_min_flag_bit8, 0) as status_code_high_cume_dist_per_k_min_flag_bit8
    from scored
    where k is distinct from 'version'
      and k_min_flag_bit8 <= 1
)
-- Native SQL UNPIVOT (wide to long)
select
    k,
    measure,
    value
from (
    select
        k,
        status_code_high_cume_dist_per_k_min_flag_bit8,
        status_code_high_cume_dist
    from base
) src
unpivot (
    value for measure in (status_code_high_cume_dist_per_k_min_flag_bit8, status_code_high_cume_dist)
)
