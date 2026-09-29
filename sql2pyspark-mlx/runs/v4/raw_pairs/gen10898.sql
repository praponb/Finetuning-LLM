select
    orders.id,
    orders.order_date,
    orders.customer_id,
    t.stddev_cost
from orders
join (select order_id, round(stddev(cost), 2) as stddev_cost
    from shipments
    where delivered_date between '2024-08-01' and '2025-09-01'
    group by order_id) t on orders.id = t.order_id
where t.stddev_cost >= 77
