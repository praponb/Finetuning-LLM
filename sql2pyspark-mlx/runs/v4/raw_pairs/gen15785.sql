SELECT status, SUM(discount) total_discount, SUM(SUM(discount)) OVER (ORDER BY status) AS cumulative_discount FROM orders GROUP BY status ORDER BY total_discount DESC;
