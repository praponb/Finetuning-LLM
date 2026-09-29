SELECT store_id, customer_id, LOWER(status) AS status_lower FROM orders WHERE discount > 0.14;
