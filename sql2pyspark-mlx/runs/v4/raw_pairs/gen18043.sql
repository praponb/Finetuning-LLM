with order_items_by_product as (
    select product_id, max(quantity) - min(quantity) as quantity_range, max(unit_price) as max_unit_price
    from order_items
    where product_id is null
    group by product_id
)
select
    p.id,
    p.supplier_id,
    p.price,
    oibp.quantity_range,
    oibp.max_unit_price
from products p
inner join order_items_by_product oibp on p.id = oibp.product_id
where oibp.quantity_range > 50;
