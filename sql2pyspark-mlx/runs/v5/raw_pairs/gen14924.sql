WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        last_restock,
        AVG(quantity) OVER (ORDER BY last_restock, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS quantity_avg_5,
        SUM(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity
    FROM inventory
),
with_dims AS (
    -- Attach products attributes
    SELECT
        s.*,
        p.stock_qty,
        p.cost
    FROM scored s
    LEFT JOIN products p
        ON s.product_id = p.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        quantity_avg_5,
        stock_qty,
        cost,
        quantity_avg_5 / NULLIF(stock_qty, 0) AS quantity_avg_5_per_stock_qty
    FROM with_dims
    WHERE last_restock >= DATE '2023-07-03'
      AND cumulative_quantity BETWEEN 535 AND 635
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(quantity_avg_5) OVER w_hist AS product_id_avg_quantity_avg_5,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY stock_qty DESC, id) AS stock_qty_bucket,
        LEAD(quantity_avg_5_per_stock_qty, 1) OVER w_hist_2 AS next_quantity_avg_5_per_stock_qty
    FROM valid
    WINDOW w_hist AS (PARTITION BY product_id), w_hist_2 AS (PARTITION BY product_id ORDER BY quantity_avg_5_per_stock_qty, id)
),
valid_inventory AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(stock_qty_bucket, quantity_avg_5) AS greatest_stock_qty_bucket_quantity_avg_5
    FROM metrics
    WHERE quantity_avg_5_per_stock_qty > 52
)
-- Multi-level aggregation
SELECT
    product_id,
    APPROX_PERCENTILE(quantity_avg_5, ARRAY(0.25, 0.5, 0.75)) AS quantity_avg_5_quartiles,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(stock_qty, 0.95) AS stock_qty_p95
FROM valid_inventory
GROUP BY product_id
ORDER BY cnt DESC NULLS FIRST;
