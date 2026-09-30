WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        city,
        rating,
        TRIM(country) AS clean_country
    FROM suppliers
    WHERE city IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) = 1
),
filtered_suppliers AS (
    -- Basic cleansing and defaults
    SELECT *, CEIL(rating) AS rating_whole
    FROM deduped
    WHERE city IN ('Singapore', 'Berlin')
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.product_price,
        latest.product_brand
    FROM filtered_suppliers s
    JOIN LATERAL (
            SELECT c.price AS product_price, c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.category IN ('Books', 'Electronics', 'Fashion')
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        rating,
        product_price,
        SUM(product_price) OVER w_running AS last3_sum_product_price,
        MAX(product_price) OVER w_running AS last3_max_product_price,
        COUNT(product_price) OVER w_running AS last3_count_product_price
    FROM kept
    WINDOW w_running AS (PARTITION BY city, product_brand ORDER BY rating_whole, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN last3_count_product_price >= 13 THEN 'HIGH' ELSE 'LOW' END AS last3_count_product_price_tier,
    ROUND(AVG(rating), 2) AS avg_rating,
    PERCENTILE_APPROX(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles
FROM scored
GROUP BY CASE WHEN last3_count_product_price >= 13 THEN 'HIGH' ELSE 'LOW' END
HAVING ROUND(AVG(rating), 2) > 0
ORDER BY avg_rating NULLS LAST
