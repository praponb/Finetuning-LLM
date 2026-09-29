SELECT channel, MAX(order_date) AS last_order_date FROM orders WHERE amount > 1040 GROUP BY channel ORDER BY channel DESC;
