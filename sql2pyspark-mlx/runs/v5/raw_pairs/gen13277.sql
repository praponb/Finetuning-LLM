WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM suppliers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating ASC, id ASC) = 1
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.country,
        latest.product_stock_qty
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        supplier_name,
        country,
        product_stock_qty
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY product_stock_qty DESC NULLS LAST) AS row_num
        FROM with_latest
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        country,
        product_stock_qty,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY product_stock_qty DESC, id ASC) AS seq_no,
        MIN(product_stock_qty) OVER (PARTITION BY country) AS country_min_product_stock_qty,
        NTILE(10) OVER (PARTITION BY country ORDER BY product_stock_qty DESC, id ASC) AS product_stock_qty_bucket,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY product_stock_qty ASC, id ASC) AS seq_no_2,
        AVG(product_stock_qty) OVER (ORDER BY product_stock_qty ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS product_stock_qty_avg_3
    FROM ranked
)
SELECT
    id,
    product_stock_qty,
    seq_no,
    country_min_product_stock_qty,
    product_stock_qty_bucket,
    seq_no_2
FROM enriched
