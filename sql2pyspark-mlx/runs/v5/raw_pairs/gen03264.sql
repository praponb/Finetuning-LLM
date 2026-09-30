WITH scored AS (
    SELECT
        id,
        country,
        city,
        NTILE(3) OVER w_running AS rating_bucket,
        LAG(rating, 1) OVER w_running_2 AS prev_rating
    FROM suppliers
    WHERE country = 'JP'
    WINDOW w_running AS (ORDER BY rating, id), w_running_2 AS (PARTITION BY city ORDER BY rating, id)
),
with_last AS (
    SELECT
        s.id,
        s.country,
        s.rating_bucket,
        s.prev_rating,
        latest.product_category
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.category AS product_category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.cost >= 660
        )
),
scored_suppliers AS (
    SELECT
        *,
        LEAD(prev_rating) OVER w_part AS next_prev_rating,
        LAG(rating_bucket) OVER w_part AS prev_rating_bucket,
        LEAD(rating_bucket) OVER w_part AS next_rating_bucket,
        ROW_NUMBER() OVER w_part AS seq_no,
        SUM(prev_rating) OVER (PARTITION BY product_category, country) AS product_category_sum_prev_rating
    FROM kept
    WINDOW w_part AS (PARTITION BY product_category ORDER BY rating_bucket, id)
),
kept_suppliers AS (
    SELECT s.*
    FROM scored_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.price > 1050
        )
)
SELECT
    country,
    CASE WHEN next_rating_bucket >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_rating_bucket_tier,
    PERCENTILE_APPROX(prev_rating_bucket, 0.5) AS prev_rating_bucket_p50,
    AVG(rating_bucket) AS avg_rating_bucket,
    COUNT_IF(product_category = 'Fashion') AS fashion_events,
    DENSE_RANK() OVER (PARTITION BY country ORDER BY PERCENTILE_APPROX(prev_rating_bucket, 0.5) DESC) AS prev_rating_bucket_p50_dense_rank
FROM kept_suppliers
GROUP BY country, CASE WHEN next_rating_bucket >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING COUNT_IF(product_category = 'Fashion') < 1000
ORDER BY avg_rating_bucket DESC
