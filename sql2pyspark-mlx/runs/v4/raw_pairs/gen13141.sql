SELECT s.region, MAX(o.discount) - MIN(o.discount) AS discount_range FROM orders o JOIN stores s ON o.store_id = s.id GROUP BY s.region ORDER BY region ASC, discount_range DESC LIMIT 5
