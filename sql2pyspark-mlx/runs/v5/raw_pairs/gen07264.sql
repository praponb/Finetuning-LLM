WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        price / NULLIF(stock_qty, 0) price_per_stock_qty,
        COALESCE(brand, 'NONE') brand_filled
    FROM products
    WHERE brand IS DISTINCT FROM 'Acme'
      AND price < 680
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        s.*,
        p.supplier_name,
        p.rating
    FROM base s
    INNER JOIN suppliers p
        ON s.supplier_id = p.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(price) OVER (PARTITION BY brand, supplier_id ORDER BY cost, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) price_avg_5,
        LAST_VALUE(price_per_stock_qty, TRUE) OVER (ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_price_per_stock_qty
    FROM with_dims
    WHERE price > 690
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.sale_date < s.launch_date
        )
),
totals AS (
    -- Grouped metrics
    SELECT
        brand,
        DATE_TRUNC('YEAR', launch_date) AS year_start,
        SORT_ARRAY(COLLECT_SET(category)) AS category_list,
        BIT_OR(stock_qty) AS stock_qty_bit_or,
        COUNT(*) AS cnt
    FROM eligible
    GROUP BY brand, DATE_TRUNC('YEAR', launch_date)
)
SELECT
    brand,
    year_start,
    category_list,
    stock_qty_bit_or,
    cnt
FROM totals
