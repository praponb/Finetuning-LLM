with prev_events as (
    select *, lag(paid_at) over (partition by method order by paid_at, id) as prev_paid_at
    from payments
),
with_gaps as (
    select *, case when prev_paid_at is null or paid_at > prev_paid_at + interval 90 days then 1 else 0 end as is_new_session
    from prev_events
),
sessions_payments as (
    select *, sum(is_new_session) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
)
select
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.status,
    s.prev_paid_at,
    s.is_new_session,
    s.burst_id,
    case when status rlike '^(REFUNDED|FAILED)$' then 'GROUP_B' when status rlike '^(SUCCESS)$' then 'CORE' else 'PRIORITY' end as status_zone,
    (select sum(x.quantity) from order_items x where x.order_id = s.order_id) as sum_order_item_quantity
from sessions_payments s
order by is_new_session desc, status;
