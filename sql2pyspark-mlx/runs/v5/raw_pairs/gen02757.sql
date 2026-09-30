WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        country,
        rating,
        ROUND(rating, 1) AS rating_rounded
    FROM suppliers
    WHERE supplier_name LIKE 'Pa%'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.cost <= 480
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        rating,
        rating_rounded,
        CUME_DIST() OVER (PARTITION BY country ORDER BY rating DESC NULLS LAST) AS rating_cume_dist,
        AVG(rating) OVER (ORDER BY rating, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rating_avg_5,
        CUME_DIST() OVER (PARTITION BY country ORDER BY rating DESC NULLS FIRST) AS rating_cume_dist_2,
        LEAD(rating_rounded, 1) OVER (PARTITION BY country ORDER BY rating, id) AS next_rating_rounded
    FROM eligible
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT ALL
    SELECT id
    FROM suppliers
    WHERE rating >= 2.1
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN matched_keys k ON s.id = k.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LEAST(rating, rating_rounded) AS least_rating_rating_rounded,
        CEIL(rating_cume_dist) AS rating_cume_dist_whole
    FROM kept_rows
    WHERE next_rating_rounded < 28
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        rating_avg_5 - rating_rounded AS rating_avg_5_minus_rating_rounded,
        rating_cume_dist_2 * least_rating_rating_rounded AS rating_cume_dist_2_times_least_rating_rating_rounded
    FROM valid
    WHERE rating_cume_dist_whole < 73
)
-- Multi-level aggregation
SELECT
    CASE WHEN least_rating_rating_rounded >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END AS least_rating_rating_rounded_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(rating_cume_dist), 2) AS avg_rating_cume_dist,
    GROUPING(CASE WHEN least_rating_rating_rounded >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_id
FROM cleaned
GROUP BY ROLLUP (CASE WHEN least_rating_rating_rounded >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END)
