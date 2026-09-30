SELECT order_date, 'order' AS source FROM orders WHERE store_id IN (20, 30, 32) UNION SELECT opened_date AS order_date, 'account' AS source FROM accounts ORDER BY source DESC
