WITH enriched AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.product_name,
        p.launch_date,
        p.stock_qty
    FROM inventory src
    JOIN products p
        ON src.product_id = p.id
    WHERE p.supplier_id IN (8, 40)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        launch_date,
        stock_qty,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY stock_qty ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS quantity_avg_3,
        LAST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity
    FROM enriched
),
scored_inventory AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        stock_qty,
        quantity_avg_3,
        last_known_quantity,
        LAG(last_known_quantity) OVER (PARTITION BY store_id ORDER BY launch_date ASC, id ASC) AS prev_last_known_quantity,
        LAG(quantity_avg_3) OVER (PARTITION BY store_id ORDER BY launch_date ASC, id ASC) AS prev_quantity_avg_3,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY launch_date ASC, id ASC) AS event_seq,
        NTILE(10) OVER (PARTITION BY store_id ORDER BY stock_qty ASC, id ASC) AS stock_qty_bucket
    FROM scored
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        stock_qty,
        event_seq,
        stock_qty_bucket,
        prev_last_known_quantity - prev_quantity_avg_3 AS prev_last_known_quantity_minus_prev_quantity_avg_3,
        ROUND(stock_qty, 0) AS stock_qty_rounded
    FROM scored_inventory
    WHERE quantity_avg_3 = 203
),
enriched_inventory AS (
    -- Running and moving aggregates
    SELECT
        id,
        stock_qty,
        event_seq,
        stock_qty_rounded,
        LEAD(stock_qty_rounded) OVER (PARTITION BY store_id ORDER BY stock_qty_bucket ASC, id ASC) AS next_stock_qty_rounded,
        LEAD(stock_qty) OVER (PARTITION BY store_id ORDER BY stock_qty_bucket ASC, id ASC) AS next_stock_qty,
        LAG(stock_qty) OVER (PARTITION BY store_id ORDER BY stock_qty_bucket ASC, id ASC) AS prev_stock_qty,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY stock_qty_bucket ASC, id ASC) AS row_seq
    FROM cleaned
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(next_stock_qty_rounded) AS next_stock_qty_rounded_whole
    FROM enriched_inventory
    WHERE next_stock_qty = 387
)
SELECT
    id,
    stock_qty_rounded,
    row_seq
FROM base
ORDER BY row_seq DESC;
