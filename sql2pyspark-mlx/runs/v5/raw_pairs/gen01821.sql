SELECT method, status, MIN(amount) AS min_amount FROM payments WHERE amount <= 780 AND (amount <= 1250 OR paid_at >= '2025-08-01') GROUP BY method, status HAVING min_amount > 1220 ORDER BY method;
