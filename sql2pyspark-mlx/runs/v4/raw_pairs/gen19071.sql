SELECT
    CASE WHEN quantity >= 234 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    ROUND(AVG(quantity), 2) AS avg_quantity_2,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    ROUND(AVG(quantity), 2) AS avg_quantity_3,
    STDDEV_SAMP(quantity) AS quantity_stddev_2,
    APPROX_PERCENTILE(quantity, 0.5) AS quantity_p50
FROM inventory
GROUP BY quantity_tier;
