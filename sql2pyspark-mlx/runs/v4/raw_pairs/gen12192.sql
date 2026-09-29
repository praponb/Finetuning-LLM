WITH with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.last_restock,
        x.quantity
    FROM products s
    JOIN LATERAL (
            SELECT c.last_restock, c.quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY stock_qty DESC) AS stock_qty_dense_rank,
        SUM(quantity) OVER w_hist AS cumulative_quantity
    FROM with_latest
    WINDOW w_hist AS (PARTITION BY supplier_id, category ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY supplier_id, brand ORDER BY price DESC) AS row_num
    FROM enriched
    QUALIFY row_num = 1
),
with_windows AS (
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
        last_restock,
        quantity,
        stock_qty_dense_rank,
        cumulative_quantity,
        row_num,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date, id) AS seq_no,
        LAG(stock_qty, 1) OVER (ORDER BY row_num, id) AS prev_stock_qty,
        NTILE(3) OVER (PARTITION BY supplier_id, brand ORDER BY cumulative_quantity DESC, id) AS cumulative_quantity_bucket
    FROM ranked
),
enriched_products AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        p.rating,
        p.city,
        p.supplier_name
    FROM with_windows src
    LEFT JOIN suppliers p
        ON src.supplier_id = p.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        supplier_id,
        category,
        stock_qty
    FROM enriched_products
) src
PIVOT (
    MAX(stock_qty) AS total,
    COUNT(1) AS item_count
    FOR category IN ('Electronics', 'Fashion')
);
