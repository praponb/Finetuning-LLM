with gaps as (
    select *, case when lag(paid_at) over (partition by status order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by status order by paid_at, id) + interval 60 days then 1 else 0 end as new_group_flag
    from payments
),
sessions_payments as (
    select *, sum(new_group_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as island_id
    from gaps
)
select
    case when island_id >= 9 then 'LARGE' else 'SMALL' end as island_id_tier,
    date_trunc('year', paid_at) as year_start,
    island_id,
    count_if(method = 'paypal') as paypal_events,
    count(distinct order_id) as distinct_order_ids,
    count_if(method = 'paypal') as paypal_events_2,
    count_if(method = 'paypal') as paypal_events_3,
    sum(case when status = 'REFUNDED' then island_id else 0 end) as refunded_island_id
from sessions_payments
group by case when island_id >= 9 then 'LARGE' else 'SMALL' end, date_trunc('year', paid_at), island_id
order by distinct_order_ids
