WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(shipping_fee * 1.07, 2) AS shipping_fee_scaled,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM orders
    WHERE store_id IN (7, 12, 13, 25)
)
-- Multi-level aggregation
SELECT
    CASE WHEN discount >= 0.45 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_tier,
    CASE WHEN amount >= 1710 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    SUM(CASE WHEN status = 'SHIPPED' THEN shipping_fee_scaled ELSE 0 END) AS shipped_shipping_fee_scaled,
    PERCENTILE_APPROX(discount, ARRAY(0.25, 0.5, 0.75)) AS discount_quartiles,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM cleaned
GROUP BY CASE WHEN discount >= 0.45 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN amount >= 1710 THEN 'HIGH' ELSE 'LOW' END
