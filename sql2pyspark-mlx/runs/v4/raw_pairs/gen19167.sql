SELECT
    QUARTER(paid_at) AS paid_quarter,
    status,
    method,
    COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count,
    MIN(paid_at) AS min_paid_at,
    MAX(amount) - MIN(amount) AS amount_range
FROM payments
WHERE status = 'SUCCESS' AND amount < 1130 AND TO_DATE(paid_at) >= '2023-11-09'
GROUP BY QUARTER(paid_at), status, method
HAVING refunded_count >= 5;
