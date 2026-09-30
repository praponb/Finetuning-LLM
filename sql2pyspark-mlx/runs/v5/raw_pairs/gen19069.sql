with exploded as (
    select
        id,
        latency_ms,
        status_code,
        k,
        v
    from app_logs
    lateral view explode(context) t as k, v
),
base as (
    select *, round(status_code * 100, 2) as status_code_scaled
    from exploded
    where k is not distinct from 'version'
      and status_code between 282 and 442
),
latest as (
    select
        id,
        latency_ms,
        k,
        v,
        status_code_scaled
    from base
    qualify rank() over (partition by k order by status_code desc) = 1
),
enriched as (
    select
        *,
        percent_rank() over w_ordered as status_code_scaled_percent_rank,
        lag(latency_ms, 2) over (order by status_code_scaled, id, k) as prev_latency_ms,
        avg(latency_ms) over (partition by k order by latency_ms, id, k rows between 2 preceding and 1 preceding) as latency_ms_avg_2_prior,
        max(status_code_scaled) over (partition by k) as k_max_status_code_scaled,
        sum(status_code_scaled) over (partition by k order by latency_ms, id, k rows between unbounded preceding and current row) as cumulative_status_code_scaled
    from latest
    window w_ordered as (partition by k order by status_code_scaled desc nulls first)
),
valid as (
    select *, case when k_max_status_code_scaled >= 68 then 'High' when k_max_status_code_scaled >= 34 then 'Medium' else 'Low' end as k_max_status_code_scaled_level
    from enriched
    where k_max_status_code_scaled >= 70
)
select
    id,
    k,
    v,
    status_code_scaled_percent_rank,
    prev_latency_ms,
    latency_ms_avg_2_prior,
    k_max_status_code_scaled,
    k_max_status_code_scaled_level
from valid
order by id desc
