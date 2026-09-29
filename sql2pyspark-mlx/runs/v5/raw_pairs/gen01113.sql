with order_stats as (
    select order_id, max(paid_at) as last_paid_at, max(amount) - min(amount) as amount_range
    from payments
    group by order_id
)
select
    o.id,
    o.shipping_fee,
    os.last_paid_at,
    os.amount_range
from orders o
join order_stats os on o.id = os.order_id
order by last_paid_at desc;
