WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        country,
        COALESCE(rating, 0) AS rating_filled,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM suppliers
    WHERE rating >= 3.2
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating_filled, id) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (ORDER BY rating_filled DESC, id) AS rating_filled_bucket,
        SUM(rating_filled) OVER w_running AS running_rating_filled,
        SUM(rating_filled) OVER w_running AS running_rating_filled_2
    FROM latest
    WINDOW w_running AS (PARTITION BY country ORDER BY rating_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT country
    FROM metrics
    INTERSECT
    SELECT country
    FROM suppliers
    WHERE rating > 4.1
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.country = k.country
)
-- Grouped metrics
SELECT
    CASE WHEN rating_filled >= 84 THEN 'LARGE' ELSE 'SMALL' END AS rating_filled_tier,
    CASE WHEN running_rating_filled >= 343 THEN 'LARGE' ELSE 'SMALL' END AS running_rating_filled_tier,
    country,
    AVG(running_rating_filled) FILTER (WHERE country = 'DE') AS de_avg_running_rating_filled,
    MIN(rating_filled) AS min_rating_filled,
    MAX(running_rating_filled_2) AS max_running_rating_filled_2
FROM verified
GROUP BY 1, 2, 3
ORDER BY min_rating_filled NULLS FIRST
