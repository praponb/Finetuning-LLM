WITH with_windows AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        AVG(quantity) OVER w_hist AS running_avg_quantity,
        COUNT(quantity) OVER w_hist AS running_count_quantity,
        MIN(quantity) OVER w_hist AS running_min_quantity,
        NTILE(4) OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) AS unit_price_bucket
    FROM order_items
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    product_id,
    CASE WHEN running_count_quantity >= 13 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_count_quantity_tier,
    CORR(quantity, unit_price) AS corr_quantity_unit_price,
    ROUND(AVG(running_avg_quantity), 2) AS avg_running_avg_quantity,
    APPROX_PERCENTILE(running_min_quantity, ARRAY(0.25, 0.5, 0.75)) AS running_min_quantity_quartiles
FROM with_windows
GROUP BY product_id, CASE WHEN running_count_quantity >= 13 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY product_id;
