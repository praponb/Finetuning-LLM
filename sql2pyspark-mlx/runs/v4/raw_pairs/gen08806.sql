select id, discount, customer_id, store_id from orders where shipping_fee <= 39 and status in ('SHIPPED', 'RETURNED', 'CANCELLED') and discount < 0.32
