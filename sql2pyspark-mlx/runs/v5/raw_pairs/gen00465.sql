with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by method order by paid_at, id) > interval 60 days then 1 else 0 end as is_new_session
    from payments
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        method,
        paid_at,
        sum(is_new_session) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from gaps
)
-- Grouped metrics
select
    case when burst_id >= 6 then 'PREMIUM' else 'STANDARD' end as burst_id_tier,
    max(burst_id) as max_burst_id,
    round(avg(burst_id), 2) as avg_burst_id,
    size(collect_set(method)) as num_methods,
    sum(case when method = 'promptpay' then burst_id else 0 end) as promptpay_burst_id,
    count_if(method = 'cash') as cash_events,
    max(paid_at) as last_seen
from sessionized
group by 1;
