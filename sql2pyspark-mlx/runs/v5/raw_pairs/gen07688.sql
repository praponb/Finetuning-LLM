WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        stock_qty,
        tags,
        supplier_id,
        CAST(price AS INT) AS price_int
    FROM products
    WHERE supplier_id IN (16, 37, 40)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        stock_qty,
        tags,
        supplier_id,
        price_int,
        FLOOR(stock_qty) AS stock_qty_whole
    FROM filtered
    WHERE supplier_id IS NOT NULL
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.unit_price
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        product_name,
        supplier_id,
        price_int,
        stock_qty_whole,
        unit_price,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY unit_price DESC, id) AS event_seq,
        SUM(stock_qty_whole) OVER (PARTITION BY supplier_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_stock_qty_whole,
        LAG(stock_qty, 2) OVER (PARTITION BY category ORDER BY stock_qty_whole, id) AS prev_stock_qty,
        RANK() OVER (PARTITION BY category ORDER BY stock_qty DESC NULLS LAST) AS stock_qty_rank,
        CUME_DIST() OVER (PARTITION BY category ORDER BY unit_price DESC NULLS LAST) AS unit_price_cume_dist
    FROM with_latest
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(cumulative_stock_qty_whole) IGNORE NULLS OVER w_ordered AS first_known_cumulative_stock_qty_whole,
        ROW_NUMBER() OVER (ORDER BY event_seq, id) AS seq_no,
        AVG(event_seq) OVER (PARTITION BY supplier_id ORDER BY prev_stock_qty, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS event_seq_avg_3,
        NTILE(5) OVER (PARTITION BY supplier_id ORDER BY unit_price_cume_dist DESC, id) AS unit_price_cume_dist_bucket,
        ROUND(stock_qty_rank / SUM(stock_qty_rank) OVER (PARTITION BY supplier_id), 4) AS stock_qty_rank_share
    FROM scored
    WINDOW w_ordered AS (PARTITION BY supplier_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    supplier_id,
    seq_no,
    PERCENTILE_APPROX(seq_no, 0.95) AS seq_no_p95,
    ROUND(AVG(stock_qty_whole), 2) AS avg_stock_qty_whole,
    GROUPING(supplier_id) AS grouping_id
FROM metrics
GROUP BY CUBE (supplier_id, seq_no);
