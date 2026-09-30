WITH kept_keys AS (
    -- Set difference on keys
    SELECT country, id
    FROM suppliers
    EXCEPT ALL
    SELECT country, id
    FROM suppliers
    WHERE city IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM suppliers s
    INNER JOIN kept_keys k ON s.country = k.country AND s.id = k.id
)
-- Multi-level aggregation
SELECT
    city,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(rating) AS total_rating,
    SUM(CASE WHEN city = 'Tokyo' THEN rating ELSE 0 END) AS tokyo_rating,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN city = 'Tokyo' THEN 1 END), 0), 2) AS rating_per_tokyo,
    APPROX_PERCENTILE(rating, 0.9) AS rating_p90,
    APPROX_PERCENTILE(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles
FROM kept_rows
GROUP BY city;
