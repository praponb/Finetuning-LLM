WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        CASE WHEN country = 'UK' THEN 1 ELSE 0 END AS is_uk
    FROM suppliers
    WHERE city IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(rating) OVER (PARTITION BY city ORDER BY rating, id) AS prev_rating,
        LEAD(rating) OVER (PARTITION BY city ORDER BY rating, id) AS next_rating,
        LEAD(is_uk) OVER (PARTITION BY city ORDER BY rating, id) AS next_is_uk,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS row_seq
    FROM cleaned
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        rating,
        is_uk,
        prev_rating,
        next_rating,
        AVG(row_seq) OVER (PARTITION BY city, country ORDER BY next_rating, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS row_seq_avg_5_prior,
        LAG(rating, 2, 0) OVER (PARTITION BY country, city ORDER BY prev_rating, id) AS prev_rating_2,
        NTILE(10) OVER (PARTITION BY country ORDER BY prev_rating DESC, id) AS prev_rating_bucket
    FROM metrics
    WHERE LOWER(TRIM(supplier_name)) = 'lion city imports'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(prev_rating) OVER (PARTITION BY city ORDER BY prev_rating, id) AS prev_prev_rating,
        LEAD(prev_rating) OVER (PARTITION BY city ORDER BY prev_rating, id) AS next_prev_rating,
        LEAD(prev_rating_2) OVER (PARTITION BY city ORDER BY prev_rating, id) AS next_prev_rating_2,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY prev_rating, id) AS row_seq
    FROM enriched
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.supplier_id IS NULL
        )
)
-- Ranking and gap features
SELECT
    id,
    city,
    rating,
    is_uk,
    next_rating,
    row_seq_avg_5_prior,
    prev_rating_2,
    SUM(next_prev_rating) OVER w_grp AS running_sum_next_prev_rating,
    AVG(next_prev_rating) OVER w_grp AS running_avg_next_prev_rating,
    MAX(next_prev_rating) OVER w_grp AS running_max_next_prev_rating,
    MIN(next_prev_rating) OVER w_grp AS running_min_next_prev_rating
FROM eligible
WINDOW w_grp AS (PARTITION BY city ORDER BY prev_rating_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
