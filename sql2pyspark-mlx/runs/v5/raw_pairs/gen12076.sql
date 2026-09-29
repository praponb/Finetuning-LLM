SELECT
    status,
    SUM(amount) AS total_amount,
    COUNT(*) AS cnt,
    SUM(SUM(amount)) OVER (ORDER BY status) AS cumulative_amount
FROM payments
GROUP BY status
