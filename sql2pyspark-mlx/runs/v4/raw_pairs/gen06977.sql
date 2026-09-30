WITH valid AS (
    -- Keep recent, valid rows
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
        SUBSTRING(category, 1, 4) category_prefix
    FROM products
    WHERE brand IS NOT NULL
      AND supplier_id = 15
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.order_id IN (1, 12)
        )
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM not_flagged
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC, id) = 1
),
valid_products AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        category_prefix,
        price / NULLIF(stock_qty, 0) price_per_stock_qty,
        COALESCE(brand, 'UNKNOWN') resolved_brand
    FROM latest
    WHERE launch_date > DATE '2024-03-26'
      AND supplier_id IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC, id) seq_no,
        NTILE(3) OVER (PARTITION BY brand ORDER BY stock_qty, id) stock_qty_bucket
    FROM valid_products
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(category, 1, 4) category_prefix_2
    FROM with_windows
    WHERE launch_date > DATE '2023-04-20'
      AND brand IS NOT NULL
      AND supplier_id = 40
),
totals AS (
    -- Grouped metrics
    SELECT
        category,
        DATE_TRUNC('MONTH', launch_date) AS activity_month,
        COUNT(*) FILTER (WHERE brand = 'Umbrella') AS umbrella_rows,
        CORR(stock_qty, price) AS corr_stock_qty_price,
        COUNT(*) FILTER (WHERE brand = 'Globex') AS globex_rows
    FROM cleaned
    GROUP BY CUBE (category, DATE_TRUNC('MONTH', launch_date))
)
SELECT
    umbrella_rows,
    corr_stock_qty_price,
    globex_rows
FROM totals
ORDER BY umbrella_rows DESC NULLS FIRST
