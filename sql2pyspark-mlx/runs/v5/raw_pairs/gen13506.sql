WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(product_name) AS product_name_length,
        DATE_FORMAT(launch_date, 'MMM yyyy') AS launch_date_label,
        COALESCE(brand, 'UNKNOWN') AS resolved_brand
    FROM products
    WHERE supplier_id IS NOT NULL
      AND brand IS DISTINCT FROM 'Globex'
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.unit_price >= 150
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.store_id IN (1, 2, 4, 6)
        )
),
eligible_products AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.unit_price <= 350
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(stock_qty) OVER (PARTITION BY category, supplier_id ORDER BY launch_date, id) AS prev_stock_qty,
        LAG(product_name_length) OVER (PARTITION BY category, supplier_id ORDER BY launch_date, id) AS prev_product_name_length,
        ROW_NUMBER() OVER (PARTITION BY category, supplier_id ORDER BY launch_date, id) AS seq_no
    FROM eligible_products
    WHERE price > 610
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY supplier_id ORDER BY seq_no NULLS LAST) AS seq_no_percent_rank,
        SUM(price) OVER (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_price
    FROM with_windows
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY price, id) = 1
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.brand,
    s.price,
    s.cost,
    s.tags,
    s.supplier_id,
    s.product_name_length,
    (SELECT SUM(p2.price) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS sum_product_price
FROM deduped s
ORDER BY supplier_id DESC NULLS FIRST
