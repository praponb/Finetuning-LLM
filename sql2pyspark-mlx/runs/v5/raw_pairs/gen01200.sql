WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        CAST(quantity AS STRING) AS quantity_str,
        DAYOFWEEK(last_restock) AS last_restock_dayofweek
    FROM inventory
    WHERE store_id IN (1, 2, 5, 6)
),
with_dims AS (
    -- Attach stores attributes
    SELECT e.*, s.opened_date
    FROM filtered e
    LEFT JOIN stores s
        ON e.store_id = s.id
    WHERE s.region = 'West'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_dims
    QUALIFY RANK() OVER (PARTITION BY store_id ORDER BY quantity NULLS FIRST) = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT store_id, id
    FROM ranked
    INTERSECT ALL
    SELECT store_id, id
    FROM inventory
    WHERE store_id = 2
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    JOIN remaining_keys k ON s.store_id = k.store_id AND s.id = k.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY product_id, store_id ORDER BY quantity NULLS FIRST) AS quantity_rank,
        DENSE_RANK() OVER w_hist AS quantity_dense_rank
    FROM verified
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY quantity DESC)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
    CASE WHEN quantity_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS quantity_rank_tier,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    PERCENTILE_APPROX(quantity_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS quantity_dense_rank_quartiles,
    GROUPING_ID() AS grouping_level
FROM metrics
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', last_restock), CASE WHEN quantity_rank >= 8 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('QUARTER', last_restock)), ())
ORDER BY quantity_rank_tier DESC;
