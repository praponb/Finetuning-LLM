WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LAST_DAY(last_restock) last_restock_month_end
    FROM inventory
    WHERE quantity >= 77
),
joined AS (
    -- Attach stores attributes
    SELECT src.*, s.city
    FROM valid src
    LEFT JOIN stores s
        ON src.store_id = s.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC NULLS LAST, id) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY city ORDER BY quantity, id) quantity_bucket,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_month_end DESC, id) seq_no
    FROM ranked
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.quantity,
        s.last_restock,
        s.quantity_bucket,
        s.seq_no,
        p.stock_qty,
        p.price,
        p.brand
    FROM with_windows s
    LEFT JOIN products p
        ON s.product_id = p.id
)
SELECT
    s.id,
    s.product_id,
    s.quantity,
    s.last_restock,
    s.quantity_bucket,
    s.stock_qty,
    s.price,
    s.brand,
    (SELECT SUM(t2.units) FROM store_sales t2 WHERE t2.product_id = s.product_id) AS sum_store_sale_units
FROM enriched s
ORDER BY id
