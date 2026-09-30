WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        FLOOR(rating) rating_whole,
        COALESCE(city, 'OTHER') city_filled
    FROM suppliers
    WHERE country IN ('DE', 'US')
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating_whole DESC NULLS FIRST, id) AS row_num
        FROM valid
    ) t
    WHERE row_num = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(rating) OVER (PARTITION BY country) country_min_rating,
        AVG(rating_whole) OVER (PARTITION BY country ORDER BY rating_whole, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) rating_whole_avg_6
    FROM latest
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.product_cost,
        latest.product_stock_qty
    FROM metrics s
    JOIN LATERAL (
            SELECT c.cost AS product_cost, c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, rating - rating_whole rating_minus_rating_whole
    FROM latest_match
    WHERE city IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.stock_qty <= 326
        )
),
valid_suppliers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        city,
        rating,
        rating_whole,
        city_filled,
        row_num,
        country_min_rating,
        rating_whole_avg_6,
        product_cost,
        product_stock_qty,
        rating_minus_rating_whole,
        rating_minus_rating_whole - product_stock_qty rating_minus_rating_whole_minus_product_stock_qty,
        COALESCE(city, 'NONE') city_filled_2
    FROM kept
    WHERE rating_whole < 33
)
-- Window metrics over several different partitions
SELECT
    *,
    MIN(country_min_rating) OVER (PARTITION BY city) city_min_country_min_rating,
    LEAD(rating_whole_avg_6, 1) OVER (ORDER BY rating_minus_rating_whole, id) next_rating_whole_avg_6
FROM valid_suppliers
