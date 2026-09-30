SELECT
    CASE WHEN discount >= 0.41 THEN 'HIGH' ELSE 'LOW' END AS discount_tier,
    APPROX_PERCENTILE(shipping_fee, 0.9) AS shipping_fee_p90,
    ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee,
    ROUND(SUM(shipping_fee) / NULLIF(COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END), 0), 2) AS shipping_fee_per_cancelled
FROM orders
GROUP BY discount_tier
ORDER BY avg_shipping_fee DESC NULLS FIRST;
