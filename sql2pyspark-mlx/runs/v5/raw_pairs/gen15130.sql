SELECT id, shipping_fee, order_date, status FROM orders WHERE amount IS NOT NULL ORDER BY order_date;
