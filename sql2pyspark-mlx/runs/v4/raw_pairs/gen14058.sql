WITH enriched AS (
    SELECT
        *,
        CORR(unit_price, quantity) OVER (PARTITION BY product_id, order_id) AS corr_unit_price_quantity,
        LAST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY product_id, order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        CORR(quantity, unit_price) OVER (PARTITION BY product_id) AS corr_quantity_unit_price
    FROM order_items
)
SELECT
    CASE WHEN quantity >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    CASE WHEN last_known_quantity >= 7 THEN 'LARGE' ELSE 'SMALL' END AS last_known_quantity_tier,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles,
    BIT_OR(quantity) AS quantity_bit_or,
    PERCENTILE_APPROX(corr_unit_price_quantity, 0.95) AS corr_unit_price_quantity_p95,
    RANK() OVER (PARTITION BY CASE WHEN quantity >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(*) DESC) AS cnt_rank
FROM enriched
GROUP BY CASE WHEN quantity >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN last_known_quantity >= 7 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY quantity_tier DESC NULLS FIRST
