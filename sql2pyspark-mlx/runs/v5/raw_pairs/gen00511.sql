SELECT c.segment, COUNT(*) AS num_orders, ROUND(STDDEV(o.discount), 2) AS stddev_discount FROM orders o JOIN customers c ON o.customer_id = c.id GROUP BY c.segment
