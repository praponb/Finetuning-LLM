with order_summary as (
    select order_id, round(stddev(quantity), 2) stddev_quantity, sum(unit_price * quantity) revenue
    from order_items
    where unit_price > 230
    group by order_id
)
select
    o.id,
    o.channel,
    o.store_id,
    os.stddev_quantity,
    os.revenue
from orders o
inner join order_summary os on o.id = os.order_id
