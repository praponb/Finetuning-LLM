WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN unit_price >= 360 THEN 'High' WHEN unit_price >= 140 THEN 'Medium' ELSE 'Low' END AS unit_price_band
    FROM order_items
    WHERE quantity <= 6
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price_band,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        DENSE_RANK() OVER w_recent AS unit_price_dense_rank
    FROM filtered
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY unit_price DESC)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CAST(unit_price AS INT) AS unit_price_int
    FROM enriched
    WHERE unit_price_dense_rank <= 7
),
filtered_order_items AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN unit_price_dense_rank >= 8 THEN 'High' WHEN unit_price_dense_rank >= 3 THEN 'Medium' ELSE 'Low' END AS unit_price_dense_rank_tier,
        unit_price_int * quantity AS unit_price_int_times_quantity
    FROM cleaned
    WHERE product_id IS NOT NULL
)
-- Grouped metrics
SELECT
    product_id,
    ROUND(AVG(unit_price_int_times_quantity), 2) AS avg_unit_price_int_times_quantity,
    SUM(unit_price_int_times_quantity) AS total_unit_price_int_times_quantity,
    ROUND(AVG(unit_price_dense_rank), 2) AS avg_unit_price_dense_rank,
    SUM(unit_price_int_times_quantity) AS total_unit_price_int_times_quantity_2
FROM filtered_order_items
GROUP BY product_id
ORDER BY total_unit_price_int_times_quantity_2 DESC;
