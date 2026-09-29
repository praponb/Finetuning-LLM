with order_items_by_product as (
    select product_id, round(stddev(unit_price), 2) as stddev_unit_price, max(quantity) as max_quantity
    from order_items
    where quantity < 3
    group by product_id
)
select
    p.id,
    p.brand,
    oibp.stddev_unit_price,
    oibp.max_quantity
from products p
left join order_items_by_product oibp on p.id = oibp.product_id
