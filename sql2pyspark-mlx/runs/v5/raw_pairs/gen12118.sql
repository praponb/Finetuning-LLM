WITH long_format AS (
    SELECT
        id,
        channel,
        measure,
        reading
    FROM (
        SELECT
            id,
            channel,
            shipping_fee,
            amount,
            discount
        FROM orders
    ) src
    UNPIVOT (
        reading FOR measure IN (shipping_fee, amount, discount)
    )
)
SELECT
    channel,
    CASE WHEN reading >= 74 THEN 'HIGH' ELSE 'LOW' END AS reading_tier,
    SUM(CASE WHEN measure = 'discount' THEN reading ELSE 0 END) AS discount_reading,
    ROUND(SUM(reading) / NULLIF(COUNT(CASE WHEN channel = 'web' THEN 1 END), 0), 2) AS reading_per_web,
    PERCENTILE_APPROX(reading, ARRAY(0.25, 0.5, 0.75)) AS reading_quartiles,
    PERCENTILE_APPROX(reading, 0.9) AS reading_p90
FROM long_format
GROUP BY 1, 2
HAVING reading_p90 > 0
ORDER BY discount_reading DESC
