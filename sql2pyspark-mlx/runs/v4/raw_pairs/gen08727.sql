SELECT
    CASE WHEN amount >= 1630 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    APPROX_PERCENTILE(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    COUNT(*) FILTER (WHERE method = 'bank_transfer') AS bank_transfer_rows,
    COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_rows,
    SUM(amount) AS total_amount,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles_2
FROM payments
GROUP BY 1
