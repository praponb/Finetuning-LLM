select
    order_items.quantity,
    orders.customer_id,
    products.price,
    products.product_name
from order_items
inner join orders on orders.id = order_items.order_id
inner join products on order_items.product_id = products.id
where orders.channel = 'store' and orders.discount between 0.28 and 0.37
order by quantity desc;
