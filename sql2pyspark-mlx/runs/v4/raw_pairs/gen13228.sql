SELECT
    DATE_TRUNC('YEAR', last_restock) AS year_start,
    product_id,
    store_id,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    APPROX_PERCENTILE(quantity, 0.95) AS quantity_p95,
    STDDEV_SAMP(quantity) AS quantity_stddev_2
FROM inventory
GROUP BY DATE_TRUNC('YEAR', last_restock), product_id, store_id;
