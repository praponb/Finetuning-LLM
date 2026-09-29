WITH scored AS (
    SELECT
        *,
        NTILE(3) OVER (ORDER BY quantity, id) AS quantity_bucket,
        SUM(unit_price) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price
    FROM order_items
)
SELECT
    order_id,
    APPROX_PERCENTILE(cumulative_unit_price, ARRAY(0.25, 0.5, 0.75)) AS cumulative_unit_price_quartiles,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    MAX(quantity_bucket) AS max_quantity_bucket
FROM scored
GROUP BY 1;
