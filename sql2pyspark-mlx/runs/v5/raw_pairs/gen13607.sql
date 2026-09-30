SELECT id, customer_id, discount, MONTH(order_date) AS order_date_month FROM orders WHERE customer_id IN (6, 26, 33, 38) AND (amount >= 1370 OR order_date >= '2024-02-01');
