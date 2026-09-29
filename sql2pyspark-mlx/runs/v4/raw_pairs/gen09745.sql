select
    p.id,
    p.product_name,
    count(oi.id) as order_item_count,
    round(stddev(oi.unit_price), 2) as stddev_unit_price
from products p
inner join order_items oi on p.id = oi.product_id
where oi.order_id in (8, 16, 29, 34)
group by p.id, p.product_name
