with with_prev as (
    select *, lag(paid_at) over (partition by method order by paid_at, id) as prev_paid_at
    from payments
),
gaps as (
    select *, case when prev_paid_at is null or paid_at > prev_paid_at + interval 14 days then 1 else 0 end as is_new_burst
    from with_prev
),
sessions_payments as (
    select *, sum(is_new_burst) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from gaps
)
select
    case when burst_id >= 8 then 'HIGH' else 'LOW' end as burst_id_tier,
    (unix_timestamp(max(prev_paid_at)) - unix_timestamp(min(prev_paid_at))) / 60 as span_minutes,
    count(case when method = 'card' then 1 end) as card_count,
    (unix_timestamp(max(prev_paid_at)) - unix_timestamp(min(prev_paid_at))) / 60 as duration_min,
    sort_array(collect_set(method)) as method_list
from sessions_payments
group by rollup (case when burst_id >= 8 then 'HIGH' else 'LOW' end)
order by card_count desc
