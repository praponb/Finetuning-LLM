SELECT id, order_date, status, store_id FROM orders WHERE order_date <= (SELECT MIN(order_date) FROM orders WHERE shipping_fee BETWEEN 14 AND 17) ORDER BY order_date;
