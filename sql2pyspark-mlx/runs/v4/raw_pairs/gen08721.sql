WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(category) AS category_upper,
        COALESCE(brand, 'DIRECT') AS brand_filled
    FROM products
    WHERE launch_date > DATE '2023-11-21'
      AND supplier_id IS NOT NULL
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.store_sale_units
    FROM base s
    JOIN LATERAL (
            SELECT c.units AS store_sale_units
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY category, supplier_id ORDER BY cost DESC NULLS LAST) AS row_num
    FROM latest_match
    QUALIFY row_num = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(stock_qty) OVER (PARTITION BY brand, category ORDER BY store_sale_units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty,
        SUM(price) OVER (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_price,
        COUNT(store_sale_units) OVER (PARTITION BY category) AS category_count_store_sale_units,
        ROUND(store_sale_units / SUM(store_sale_units) OVER (PARTITION BY brand), 4) AS store_sale_units_share
    FROM deduped
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.brand,
    s.price,
    s.stock_qty,
    s.launch_date,
    s.tags,
    s.supplier_id,
    (SELECT SUM(p2.stock_qty) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS sum_product_stock_qty
FROM enriched s
ORDER BY sum_product_stock_qty DESC NULLS FIRST;
