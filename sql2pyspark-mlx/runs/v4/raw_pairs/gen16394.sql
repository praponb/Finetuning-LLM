SELECT TO_DATE(paid_at) AS paid_day, MIN(amount) AS min_amount
FROM payments
GROUP BY TO_DATE(paid_at);
