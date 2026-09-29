select
    orders.customer_id,
    orders.store_id,
    orders.status,
    order_items.quantity,
    order_items.order_id
from orders
join order_items on order_items.order_id = orders.id
where orders.amount > 1260 and order_items.unit_price between 210 and 230;
