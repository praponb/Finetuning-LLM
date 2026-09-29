select
    products.id,
    products.brand,
    products.product_name,
    t.max_quantity,
    t.order_item_count
from products
left join (select product_id, max(quantity) as max_quantity, count(*) as order_item_count
    from order_items
    group by product_id) t on products.id = t.product_id
where t.max_quantity > 3.4
order by product_name;
