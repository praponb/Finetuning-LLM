WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC NULLS FIRST, id) AS latest_rank
    FROM order_items
    QUALIFY latest_rank = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, quantity + unit_price line_total
    FROM ranked
    WHERE product_id IS NOT NULL
),
with_dims AS (
    -- Attach products attributes
    SELECT s.*, d.stock_qty
    FROM base s
    JOIN products d
        ON s.product_id = d.id
)
SELECT
    id,
    unit_price,
    latest_rank,
    line_total,
    stock_qty
FROM with_dims
ORDER BY line_total
