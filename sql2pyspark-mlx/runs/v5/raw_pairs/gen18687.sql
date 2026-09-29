with order_stats as (
    select order_id, count(quantity) as quantity_count, max(quantity) as max_quantity
    from order_items
    where quantity between 6 and 8
    group by order_id
)
select
    orders.discount,
    orders.store_id,
    order_stats.quantity_count,
    order_stats.max_quantity
from orders
join order_stats on orders.id = order_stats.order_id
order by orders.discount
