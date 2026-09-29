SELECT channel, SUM(discount) total_discount, SUM(SUM(discount)) OVER (ORDER BY channel) AS cumulative_discount FROM orders GROUP BY channel ORDER BY cumulative_discount;
