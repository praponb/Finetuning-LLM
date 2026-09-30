WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        DENSE_RANK() OVER (PARTITION BY store_id, product_id ORDER BY quantity) AS quantity_dense_rank,
        AVG(quantity) OVER w_grp AS quantity_avg_2,
        NTILE(5) OVER (ORDER BY quantity, id) AS quantity_bucket
    FROM inventory
    WINDOW w_grp AS (PARTITION BY store_id, product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, product_id
    FROM enriched
    EXCEPT ALL
    SELECT id, product_id
    FROM inventory
    WHERE store_id IN (2, 4, 6)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.id = k.id AND s.product_id = k.product_id
),
enriched_inventory AS (
    -- Attach products attributes
    SELECT e.*, d.stock_qty
    FROM kept_rows e
    LEFT JOIN products d
        ON e.product_id = d.id
    WHERE d.cost BETWEEN 320 AND 440
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        product_id,
        quantity,
        last_restock,
        quantity_dense_rank,
        quantity_avg_2,
        quantity_bucket,
        stock_qty,
        GREATEST(quantity_avg_2, stock_qty) AS greatest_quantity_avg_2_stock_qty
    FROM enriched_inventory
    WHERE last_restock >= TO_DATE('2023-06-08')
      AND MONTH(last_restock) IN (4, 11, 12)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(quantity) OVER (PARTITION BY product_id) AS product_id_sum_quantity,
        RANK() OVER w_running AS quantity_dense_rank_rank,
        DENSE_RANK() OVER w_running AS quantity_dense_rank_dense_rank
    FROM valid
    WINDOW w_running AS (PARTITION BY product_id ORDER BY quantity_dense_rank DESC NULLS FIRST)
)
-- Join a dimension and aggregate
SELECT
    dim.category,
    COUNT(*) AS rows_total,
    MIN(a.stock_qty) AS min_stock_qty,
    MIN(a.quantity_bucket) AS min_quantity_bucket,
    GROUPING_ID() AS grouping_level
FROM with_windows a
INNER JOIN products dim ON a.product_id = dim.id
GROUP BY ROLLUP (dim.category)
ORDER BY min_quantity_bucket NULLS FIRST;
