WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        city,
        LENGTH(country) AS country_length,
        LENGTH(city) AS city_length
    FROM suppliers
    WHERE city <> 'New York'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY country_length, id) AS dedup_rank
        FROM valid
    ) t
    WHERE dedup_rank = 1
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.launch_date
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY launch_date DESC, id) AS rn
    FROM with_latest
    QUALIFY rn = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    CASE WHEN city_length >= 70 THEN 'PREMIUM' ELSE 'STANDARD' END AS city_length_tier,
    PERCENTILE_APPROX(city_length, 0.5) AS city_length_p50,
    COUNT_IF(country = 'DE') AS de_events,
    PERCENTILE_APPROX(city_length, 0.5) AS city_length_p50_2,
    COUNT(*) AS row_count
FROM latest
GROUP BY DATE_TRUNC('YEAR', launch_date), CASE WHEN city_length >= 70 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY city_length_p50_2 DESC NULLS LAST
