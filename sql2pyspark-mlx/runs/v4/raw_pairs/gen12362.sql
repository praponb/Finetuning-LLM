SELECT
    CASE WHEN discount >= 0.45 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_tier,
    AVG(amount) FILTER (WHERE status = 'COMPLETED') AS completed_avg_amount,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    ROUND(SUM(shipping_fee) / NULLIF(COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END), 0), 2) AS shipping_fee_per_completed,
    ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee,
    COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_rows
FROM orders
GROUP BY discount_tier
ORDER BY completed_avg_amount DESC NULLS LAST
