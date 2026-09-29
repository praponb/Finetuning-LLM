WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        PERCENT_RANK() OVER w_grp AS stock_qty_percent_rank,
        DENSE_RANK() OVER (PARTITION BY brand, supplier_id ORDER BY cost DESC) AS cost_dense_rank
    FROM products
    WINDOW w_grp AS (PARTITION BY category ORDER BY stock_qty DESC NULLS LAST)
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.inventory_last_restock
    FROM scored s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY cost_dense_rank DESC, id ASC) AS row_seq,
        SUM(price) OVER (PARTITION BY category ORDER BY price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price,
        LAST_VALUE(stock_qty) IGNORE NULLS OVER (PARTITION BY supplier_id ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_stock_qty
    FROM with_latest
)
-- Grouped metrics
SELECT
    CASE WHEN stock_qty_percent_rank >= 0.61 THEN 'PREMIUM' ELSE 'STANDARD' END AS stock_qty_percent_rank_tier,
    APPROX_PERCENTILE(stock_qty_percent_rank, 0.95) AS stock_qty_percent_rank_p95,
    BIT_AND(stock_qty) AS stock_qty_bit_and,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    ROUND(SUM(last_known_stock_qty) / NULLIF(COUNT(CASE WHEN brand = 'Acme' THEN 1 END), 0), 2) AS last_known_stock_qty_per_acme,
    ROUND(AVG(cost), 2) AS avg_cost,
    SUM(CASE WHEN brand = 'Acme' THEN row_seq ELSE 0 END) AS acme_row_seq
FROM metrics
GROUP BY CASE WHEN stock_qty_percent_rank >= 0.61 THEN 'PREMIUM' ELSE 'STANDARD' END;
