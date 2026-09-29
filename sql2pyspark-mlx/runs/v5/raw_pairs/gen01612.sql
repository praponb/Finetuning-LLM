WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        CASE WHEN quantity >= 203 THEN 'High' WHEN quantity >= 96 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM inventory
    WHERE store_id IN (2, 3, 4)
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.last_restock,
        e.quantity_tier,
        p.stock_qty,
        p.product_name,
        p.brand
    FROM base e
    LEFT JOIN products p
        ON e.product_id = p.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY stock_qty NULLS LAST) AS stock_qty_cume_dist,
        SUM(stock_qty) OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_stock_qty,
        NTILE(5) OVER (PARTITION BY product_id, brand ORDER BY stock_qty, id) AS stock_qty_bucket
    FROM with_dims
),
joined AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.stock_qty,
        src.brand,
        src.cumulative_stock_qty,
        b.category,
        b.price,
        b.brand AS product_brand
    FROM metrics src
    LEFT JOIN products b
        ON src.product_id = b.id
)
SELECT
    id,
    product_id,
    brand,
    cumulative_stock_qty,
    category
FROM joined
ORDER BY product_id;
