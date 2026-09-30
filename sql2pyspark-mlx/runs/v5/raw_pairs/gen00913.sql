SELECT status, SUM(discount) AS total_discount, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(discount) DESC) AS discount_rank FROM orders GROUP BY status ORDER BY total_discount DESC
