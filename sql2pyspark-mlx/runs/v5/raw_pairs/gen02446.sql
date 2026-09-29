WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        DAY(last_restock) AS last_restock_day
    FROM inventory
    WHERE last_restock >= DATE '2023-03-24'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        store_id,
        quantity
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC, id) = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(quantity) AS quantity_whole,
        COALESCE(quantity, 0) AS quantity_filled
    FROM deduped
    WHERE quantity IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        quantity_whole,
        LEAD(quantity_whole, 1) OVER w_grp AS next_quantity_whole,
        SUM(quantity) OVER w_recent AS cumulative_quantity,
        MAX(quantity) OVER (PARTITION BY store_id) AS store_id_max_quantity
    FROM base
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY quantity_filled, id), w_recent AS (PARTITION BY store_id ORDER BY quantity_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    CASE WHEN next_quantity_whole >= 90 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_quantity_whole_tier,
    PERCENTILE_APPROX(next_quantity_whole, ARRAY(0.25, 0.5, 0.75)) AS next_quantity_whole_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(quantity_whole), 2) AS avg_quantity_whole,
    COUNT(DISTINCT id) AS distinct_ids_2,
    COUNT(DISTINCT id) AS distinct_ids_3,
    AVG(next_quantity_whole) AS avg_next_quantity_whole
FROM scored
GROUP BY CASE WHEN next_quantity_whole >= 90 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING distinct_ids_3 >= 1
