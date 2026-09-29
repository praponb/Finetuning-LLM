select orders.amount, orders.shipping_fee, order_items.product_id, order_items.quantity from orders left join order_items on order_items.order_id = orders.id order by quantity
