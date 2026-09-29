SELECT SUM(CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END) AS shipped_count, ROUND(STDDEV(discount), 2) AS stddev_discount FROM orders ORDER BY stddev_discount DESC;
