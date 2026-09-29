WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(city) AS city_lower
    FROM suppliers
    WHERE rating >= 3.5
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        rating,
        city_lower,
        LAG(rating, 1) OVER w_grp AS prev_rating,
        LEAD(rating, 1) OVER (PARTITION BY city ORDER BY rating, id) AS next_rating
    FROM base
    WINDOW w_grp AS (PARTITION BY country ORDER BY rating, id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        city_lower,
        prev_rating,
        NTILE(4) OVER (ORDER BY rating DESC, id) AS rating_bucket,
        CUME_DIST() OVER (ORDER BY rating DESC NULLS FIRST) AS rating_cume_dist
    FROM with_windows
    WHERE next_rating <= 3.2
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND ARRAY_CONTAINS(x.tags, 'eco')
        )
),
enriched_suppliers AS (
    -- Ranking and gap features
    SELECT
        id,
        city_lower,
        prev_rating,
        rating_bucket,
        SUM(rating_cume_dist) OVER (ORDER BY rating_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating_cume_dist,
        CUME_DIST() OVER (ORDER BY rating_bucket DESC NULLS LAST) AS rating_bucket_cume_dist,
        NTILE(10) OVER (ORDER BY rating_bucket DESC, id) AS rating_bucket_bucket,
        SUM(prev_rating) OVER (ORDER BY prev_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_rating,
        AVG(prev_rating) OVER (ORDER BY rating_cume_dist, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS prev_rating_avg_3
    FROM kept
)
SELECT
    id,
    city_lower,
    prev_rating,
    rating_bucket,
    rating_bucket_cume_dist,
    rating_bucket_bucket,
    prev_rating_avg_3
FROM enriched_suppliers
ORDER BY id DESC, prev_rating
