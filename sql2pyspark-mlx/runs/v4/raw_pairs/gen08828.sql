select id, order_date, shipping_fee, customer_id, datediff(current_date(), order_date) as days_since_order_date from orders where discount > 0.23;
