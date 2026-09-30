WITH filtered AS (
    SELECT
        id,
        product_name,
        category,
        stock_qty,
        tags,
        supplier_id,
        ROUND(price * 100, 2) adjusted_price,
        LAST_DAY(launch_date) launch_date_month_end
    FROM products
    WHERE launch_date >= TO_DATE('2023-05-06')
      AND supplier_id IS NOT NULL
),
enriched AS (
    SELECT
        e.*,
        d.country,
        d.city
    FROM filtered e
    LEFT JOIN suppliers d
        ON e.supplier_id = d.id
    WHERE d.supplier_name NOT LIKE 'T%'
),
enriched_products AS (
    SELECT
        id,
        category,
        stock_qty,
        adjusted_price,
        launch_date_month_end,
        country,
        MIN(adjusted_price) OVER (PARTITION BY city) city_min_adjusted_price,
        LAG(adjusted_price, 2) OVER (PARTITION BY category ORDER BY launch_date_month_end, id) prev_adjusted_price,
        ROW_NUMBER() OVER (ORDER BY adjusted_price DESC, id) row_seq
    FROM enriched
),
valid AS (
    SELECT
        id,
        category,
        stock_qty,
        adjusted_price,
        launch_date_month_end,
        country,
        city_min_adjusted_price,
        prev_adjusted_price,
        prev_adjusted_price + adjusted_price prev_adjusted_price_plus_adjusted_price,
        COALESCE(country, 'N/A') country_clean
    FROM enriched_products
    WHERE stock_qty > 176
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY prev_adjusted_price_plus_adjusted_price, id) AS row_num
    FROM valid
    QUALIFY row_num = 1
),
not_flagged AS (
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date_month_end
        )
),
kept AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.last_restock >= s.launch_date_month_end
        )
)
SELECT
    country,
    SIZE(COLLECT_SET(category)) AS num_categories,
    COUNT(CASE WHEN category = 'Books' THEN 1 END) AS books_count,
    SUM(CASE WHEN category = 'Home' THEN stock_qty ELSE 0 END) AS home_stock_qty,
    COUNT(CASE WHEN category = 'Sports' THEN 1 END) AS sports_count
FROM kept
GROUP BY ROLLUP (country)
HAVING home_stock_qty > 0
ORDER BY books_count
