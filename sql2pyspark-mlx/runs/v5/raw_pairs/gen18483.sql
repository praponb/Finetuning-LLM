with valid as (
    -- Filter the raw rows and derive helper columns
    select *, round(amount * 1.15, 2) as amount_scaled
    from payments
    where method is not null
),
prev_events as (
    -- Previous event time per key
    select *, lag(paid_at) over (partition by status order by paid_at, id) as prev_paid_at
    from valid
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_paid_at is null or paid_at > prev_paid_at + interval 7 days then 1 else 0 end as is_new_burst
    from prev_events
),
sessions_payments as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_id
    from gaps
)
-- Aggregate with conditional metrics
select
    case when is_new_burst >= 1 then 'PREMIUM' else 'STANDARD' end as is_new_burst_tier,
    size(collect_set(method)) as num_methods,
    count(*) as num_rows,
    count(case when status = 'SUCCESS' then 1 end) as success_count,
    count(*) as event_count,
    approx_percentile(amount_scaled, array(0.25, 0.5, 0.75)) as amount_scaled_quartiles
from sessions_payments
group by case when is_new_burst >= 1 then 'PREMIUM' else 'STANDARD' end
