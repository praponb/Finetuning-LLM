SELECT
    product_id,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    SUM(quantity) AS total_quantity,
    PERCENTILE_APPROX(quantity, 0.5) AS quantity_p50,
    STDDEV_SAMP(quantity) AS quantity_stddev_2
FROM inventory
GROUP BY product_id
