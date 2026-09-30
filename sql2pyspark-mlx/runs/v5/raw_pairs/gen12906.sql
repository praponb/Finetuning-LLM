WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_quantity,
        MIN(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_quantity,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_quantity
    FROM inventory
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
    APPROX_PERCENTILE(last3_count_quantity, ARRAY(0.25, 0.5, 0.75)) AS last3_count_quantity_quartiles,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    BIT_OR(quantity) AS quantity_bit_or,
    PERCENTILE_APPROX(last3_min_quantity, 0.5) AS last3_min_quantity_p50,
    ROUND(AVG(last3_min_quantity), 2) AS avg_last3_min_quantity,
    PERCENTILE_APPROX(last3_min_quantity, ARRAY(0.25, 0.5, 0.75)) AS last3_min_quantity_quartiles
FROM metrics
GROUP BY DATE_TRUNC('QUARTER', last_restock)
