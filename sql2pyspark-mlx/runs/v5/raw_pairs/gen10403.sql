SELECT s.region, MIN(o.order_date) AS min_order_date FROM orders o INNER JOIN stores s ON o.store_id = s.id LEFT JOIN customers c ON o.customer_id = c.id GROUP BY s.region;
