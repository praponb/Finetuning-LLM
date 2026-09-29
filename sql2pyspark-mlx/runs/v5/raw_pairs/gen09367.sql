SELECT
    method,
    SUM(amount) AS total_amount,
    MAX(amount) - MIN(amount) AS amount_range,
    ROUND((100 * SUM(CASE WHEN method = 'bank_transfer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_bank_transfer
FROM payments
GROUP BY 1
