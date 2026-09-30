WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN rating >= 4.3 THEN 'High' WHEN rating >= 2.5 THEN 'Medium' ELSE 'Low' END AS rating_tier,
        TRIM(supplier_name) AS clean_supplier_name
    FROM suppliers
    WHERE city = 'Chiang Mai'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        rating_tier,
        clean_supplier_name
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating DESC, id ASC) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        country,
        city,
        LAG(rating) OVER (PARTITION BY city ORDER BY rating ASC, id ASC) AS prev_rating,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating ASC, id ASC) AS row_seq,
        AVG(rating) OVER (ORDER BY rating ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rating_avg_2
    FROM latest
    WHERE country = 'SG'
),
latest_suppliers AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating_avg_2 ASC, id ASC) = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_rating >= 4.1 THEN 'HIGH' ELSE 'LOW' END AS prev_rating_tier,
    country,
    APPROX_PERCENTILE(row_seq, 0.5) AS row_seq_p50,
    ROUND(SUM(rating_avg_2) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS rating_avg_2_per_new_york,
    COUNT_IF(city = 'Singapore') AS singapore_events,
    APPROX_PERCENTILE(prev_rating, 0.95) AS prev_rating_p95,
    GROUPING(CASE WHEN prev_rating >= 4.1 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM latest_suppliers
GROUP BY GROUPING SETS ((CASE WHEN prev_rating >= 4.1 THEN 'HIGH' ELSE 'LOW' END, country), (CASE WHEN prev_rating >= 4.1 THEN 'HIGH' ELSE 'LOW' END), ())
HAVING prev_rating_p95 > 0;
