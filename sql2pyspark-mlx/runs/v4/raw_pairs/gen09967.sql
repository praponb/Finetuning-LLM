SELECT
    CASE WHEN amount >= 1750 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    order_id,
    STDDEV_SAMP(amount) AS amount_stddev,
    COUNT(DISTINCT status) AS distinct_status,
    MAX(amount) AS max_amount,
    COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success_rows,
    SIZE(COLLECT_SET(method)) AS num_methods
FROM payments
GROUP BY amount_tier, order_id
