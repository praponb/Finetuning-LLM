WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        CASE WHEN category = 'Sports' THEN 1 ELSE 0 END AS is_sports
    FROM products
    WHERE brand IS NOT NULL
      AND brand IS DISTINCT FROM 'Globex'
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT e.*, b.supplier_name
    FROM base e
    LEFT JOIN suppliers b
        ON e.supplier_id <=> b.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        price,
        stock_qty,
        tags,
        supplier_id,
        LEAD(price) OVER w_hist AS next_price,
        LAG(price) OVER w_hist AS prev_price,
        LAG(is_sports) OVER w_hist AS prev_is_sports,
        LEAD(is_sports) OVER w_hist AS next_is_sports,
        ROW_NUMBER() OVER w_hist AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY is_sports, id) AS row_seq_2
    FROM with_dims
    WINDOW w_hist AS (PARTITION BY supplier_id, brand ORDER BY launch_date, id)
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.last_restock BETWEEN '2024-04-01' AND '2025-02-16'
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT supplier_id, id
    FROM eligible
    EXCEPT
    SELECT supplier_id, id
    FROM products
    WHERE YEAR(launch_date) >= 2025
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    JOIN remaining_keys k ON s.supplier_id = k.supplier_id AND s.id = k.id
),
remaining_keys_2 AS (
    -- Set difference on keys
    SELECT id, supplier_id
    FROM verified
    EXCEPT DISTINCT
    SELECT id, supplier_id
    FROM products
    WHERE product_name LIKE 'L%'
),
verified_products AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    JOIN remaining_keys_2 k ON s.id = k.id AND s.supplier_id = k.supplier_id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        price,
        stock_qty,
        row_seq,
        CUME_DIST() OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC NULLS FIRST) AS stock_qty_cume_dist,
        MAX(next_is_sports) OVER (PARTITION BY supplier_id) AS supplier_id_max_next_is_sports
    FROM verified_products
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN stock_qty_cume_dist >= 0.65 THEN 'HIGH' ELSE 'LOW' END AS stock_qty_cume_dist_tier,
        COUNT(*) AS event_count,
        CORR(price, stock_qty) AS corr_price_stock_qty,
        BIT_OR(stock_qty) AS stock_qty_bit_or,
        PERCENTILE_APPROX(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles,
        ROUND(AVG(stock_qty), 2) AS avg_stock_qty
    FROM with_windows
    GROUP BY stock_qty_cume_dist_tier
    HAVING avg_stock_qty > 0
)
SELECT
    stock_qty_cume_dist_tier,
    corr_price_stock_qty,
    stock_qty_bit_or,
    avg_stock_qty
FROM summary
