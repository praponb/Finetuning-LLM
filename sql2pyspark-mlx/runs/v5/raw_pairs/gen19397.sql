SELECT method, amount_range FROM (SELECT method, MAX(amount) - MIN(amount) AS amount_range FROM payments WHERE paid_at < '2025-01-01' GROUP BY method) AS agg WHERE amount_range < 61 ORDER BY method
