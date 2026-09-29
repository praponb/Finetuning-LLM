WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(rating, 1) AS rating_rounded,
        COALESCE(city, 'UNKNOWN') AS city_clean
    FROM suppliers
    WHERE city IS NOT NULL
      AND supplier_name NOT LIKE 'S%'
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.launch_date
    FROM base s
    JOIN LATERAL (
            SELECT c.launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.city,
        s.rating,
        s.city_clean,
        x.product_category,
        x.cost
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.category AS product_category, c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(product_category)) AS product_category_title,
        LENGTH(city) AS city_length,
        COALESCE(product_category, 'DIRECT') AS resolved_product_category
    FROM latest_match
    WHERE product_category IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN city_length >= 65 THEN 'LARGE' ELSE 'SMALL' END AS city_length_tier,
    APPROX_PERCENTILE(city_length, 0.5) AS city_length_p50,
    APPROX_PERCENTILE(city_length, ARRAY(0.25, 0.5, 0.75)) AS city_length_quartiles,
    APPROX_PERCENTILE(city_length, ARRAY(0.25, 0.5, 0.75)) AS city_length_quartiles_2,
    SUM(cost) FILTER (WHERE product_category = 'Books') AS books_total_cost
FROM valid
GROUP BY CASE WHEN city_length >= 65 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY city_length_p50 DESC
