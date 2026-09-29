SELECT
    CASE WHEN quantity >= 246 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    BIT_OR(quantity) AS quantity_bit_or,
    APPROX_PERCENTILE(quantity, 0.5) AS quantity_p50,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    ROUND(AVG(quantity), 2) AS avg_quantity
FROM inventory
GROUP BY CASE WHEN quantity >= 246 THEN 'LARGE' ELSE 'SMALL' END
