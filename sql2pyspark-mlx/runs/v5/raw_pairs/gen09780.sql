SELECT
    method,
    status,
    CASE WHEN amount >= 1460 THEN 'high' WHEN amount >= 450 THEN 'mid' ELSE 'low' END AS amount_bucket,
    ROUND(AVG(amount), 2) AS avg_amount,
    MIN(paid_at) AS first_paid_at
FROM payments
WHERE method IN ('promptpay', 'card')
GROUP BY 1, 2, 3
HAVING ROUND(AVG(amount), 2) >= 26;
