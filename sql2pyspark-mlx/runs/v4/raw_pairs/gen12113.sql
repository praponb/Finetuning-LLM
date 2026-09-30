WITH valid AS (
    SELECT
        id,
        order_id,
        quantity,
        GREATEST(quantity, unit_price) AS greatest_quantity_unit_price
    FROM order_items
    WHERE unit_price <= 200
),
filtered AS (
    SELECT
        *,
        FLOOR(greatest_quantity_unit_price) AS greatest_quantity_unit_price_whole,
        CASE WHEN greatest_quantity_unit_price >= 64 THEN 'High' WHEN greatest_quantity_unit_price >= 17 THEN 'Medium' ELSE 'Low' END AS greatest_quantity_unit_price_band
    FROM valid
    WHERE order_id IN (2, 6, 11)
),
with_windows AS (
    SELECT
        *,
        AVG(quantity) OVER (PARTITION BY order_id) AS order_id_avg_quantity,
        SUM(quantity) OVER (PARTITION BY order_id) AS order_id_sum_quantity
    FROM filtered
),
cleaned AS (
    SELECT
        id,
        quantity,
        greatest_quantity_unit_price,
        greatest_quantity_unit_price_whole,
        greatest_quantity_unit_price_band,
        order_id_avg_quantity,
        order_id_sum_quantity,
        ROUND(order_id_avg_quantity * 0.85, 2) AS order_id_avg_quantity_scaled
    FROM with_windows
    WHERE quantity BETWEEN 6 AND 8
)
SELECT
    CASE WHEN greatest_quantity_unit_price >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END AS greatest_quantity_unit_price_tier,
    CASE WHEN order_id_avg_quantity_scaled >= 75 THEN 'LARGE' ELSE 'SMALL' END AS order_id_avg_quantity_scaled_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(greatest_quantity_unit_price_whole) AS avg_greatest_quantity_unit_price_whole,
    PERCENTILE_APPROX(order_id_avg_quantity_scaled, 0.9) AS order_id_avg_quantity_scaled_p90,
    PERCENTILE_APPROX(greatest_quantity_unit_price_whole, 0.5) AS greatest_quantity_unit_price_whole_p50
FROM cleaned
GROUP BY GROUPING SETS ((CASE WHEN greatest_quantity_unit_price >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN order_id_avg_quantity_scaled >= 75 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN greatest_quantity_unit_price >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY order_id_avg_quantity_scaled_tier DESC;
