WITH with_dims AS (
    -- Attach stores attributes
    SELECT e.*, d.region
    FROM inventory e
    LEFT JOIN stores d
        ON e.store_id = d.id
),
joined AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.last_restock,
        s.region,
        b.stock_qty,
        b.launch_date,
        b.category
    FROM with_dims s
    LEFT JOIN products b
        ON s.product_id = b.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(stock_qty, 1) OVER w_ordered next_stock_qty,
        LEAD(stock_qty, 2) OVER (ORDER BY last_restock, id) next_stock_qty_2
    FROM joined
    WHERE last_restock BETWEEN '2023-06-01' AND '2024-10-01'
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY stock_qty, id)
),
enriched AS (
    -- Attach stores attributes
    SELECT src.*, s.store_name
    FROM metrics src
    LEFT JOIN stores s
        ON src.store_id = s.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', last_restock) AS year_start,
    COUNT_IF(category = 'Fashion') AS fashion_events,
    MAX(next_stock_qty) AS max_next_stock_qty,
    COUNT(*) FILTER (WHERE category = 'Electronics') AS electronics_rows
FROM enriched
GROUP BY 1
ORDER BY electronics_rows DESC NULLS FIRST
