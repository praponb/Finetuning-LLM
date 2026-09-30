SELECT id, method, amount, ROUND(amount / SUM(amount) OVER (PARTITION BY method), 3) AS amount_share FROM payments WHERE HOUR(paid_at) BETWEEN 11 AND 18
