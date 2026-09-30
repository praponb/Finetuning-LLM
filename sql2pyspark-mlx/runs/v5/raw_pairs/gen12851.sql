WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        quantity,
        unit_price,
        CEIL(quantity) quantity_whole
    FROM order_items
    WHERE unit_price BETWEEN 180 AND 230
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity,
        unit_price,
        quantity_whole,
        MIN(quantity_whole) OVER (ORDER BY quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_quantity_whole,
        COUNT(quantity_whole) OVER (ORDER BY quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_quantity_whole,
        MAX(quantity_whole) OVER (ORDER BY quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_quantity_whole,
        AVG(quantity_whole) OVER (ORDER BY quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_quantity_whole
    FROM cleaned
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        quantity,
        unit_price,
        quantity_whole,
        running_min_quantity_whole,
        running_count_quantity_whole,
        running_max_quantity_whole,
        running_avg_quantity_whole,
        CAST(running_avg_quantity_whole AS INT) running_avg_quantity_whole_int,
        CAST(running_avg_quantity_whole AS INT) running_avg_quantity_whole_int_2
    FROM with_windows
    WHERE quantity >= 6
),
cleaned_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        quantity,
        unit_price,
        running_count_quantity_whole,
        running_max_quantity_whole,
        running_avg_quantity_whole_int,
        COALESCE(unit_price, 0) unit_price_filled
    FROM filtered
    WHERE running_avg_quantity_whole_int > 37
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        running_count_quantity_whole,
        running_max_quantity_whole,
        running_avg_quantity_whole_int,
        unit_price_filled,
        LAST_VALUE(running_count_quantity_whole) IGNORE NULLS OVER (ORDER BY unit_price_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_running_count_quantity_whole,
        FIRST_VALUE(unit_price_filled) IGNORE NULLS OVER (ORDER BY unit_price_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_unit_price_filled
    FROM cleaned_order_items
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, first_known_unit_price_filled - quantity first_known_unit_price_filled_minus_quantity
    FROM scored
    WHERE running_avg_quantity_whole_int = 54
)
-- Grouped metrics
SELECT
    CASE WHEN last_known_running_count_quantity_whole >= 18 THEN 'PREMIUM' ELSE 'STANDARD' END AS last_known_running_count_quantity_whole_tier,
    CASE WHEN quantity >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    ROUND(AVG(unit_price_filled), 2) AS avg_unit_price_filled,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(running_count_quantity_whole), 2) AS avg_running_count_quantity_whole,
    SUM(running_count_quantity_whole) AS total_running_count_quantity_whole,
    SUM(first_known_unit_price_filled) AS total_first_known_unit_price_filled
FROM base
GROUP BY CASE WHEN last_known_running_count_quantity_whole >= 18 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN quantity >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY last_known_running_count_quantity_whole_tier DESC
