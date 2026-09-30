with with_gaps as (
    select *, case when lag(paid_at) over (partition by order_id order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by order_id order by paid_at, id) + interval 7 days then 1 else 0 end as session_start_flag
    from payments
),
islands as (
    select *, sum(session_start_flag) over (partition by order_id order by paid_at, id rows between unbounded preceding and current row) as session_id
    from with_gaps
)
select
    date_trunc('week', paid_at) as week_start,
    case when amount >= 1330 then 'HIGH' else 'LOW' end as amount_tier,
    session_id,
    count(distinct order_id) as distinct_order_ids,
    (unix_timestamp(max(paid_at)) - unix_timestamp(min(paid_at))) / 60 as duration_min,
    count(distinct method) as distinct_methods,
    sum(case when status = 'REFUNDED' then session_start_flag else 0 end) as refunded_session_start_flag
from islands
group by grouping sets ((date_trunc('week', paid_at), case when amount >= 1330 then 'HIGH' else 'LOW' end, session_id), (date_trunc('week', paid_at), case when amount >= 1330 then 'HIGH' else 'LOW' end), (date_trunc('week', paid_at)), ())
order by week_start desc
