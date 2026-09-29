WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        TRIM(city) AS clean_city,
        COALESCE(city, 'N/A') AS resolved_city
    FROM suppliers
    WHERE city IS NOT NULL
      AND country = 'DE'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        clean_city,
        resolved_city,
        DENSE_RANK() OVER w_running AS rating_dense_rank,
        AVG(rating) OVER (PARTITION BY country ORDER BY rating ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rating_avg_2,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share
    FROM cleaned
    WINDOW w_running AS (PARTITION BY city ORDER BY rating ASC NULLS FIRST)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating_share DESC, id ASC) AS dedup_rank
        FROM metrics
    ) ranked
    WHERE dedup_rank = 1
)
-- Multi-level aggregation
SELECT
    country,
    STDDEV_SAMP(rating) AS rating_stddev,
    MAX(rating_avg_2) AS max_rating_avg_2,
    COUNT(*) AS cnt
FROM latest
GROUP BY 1
HAVING cnt > 1
