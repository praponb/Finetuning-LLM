WITH base AS (
    SELECT
        *,
        CONCAT(country, ' ', supplier_name) AS country_supplier_name,
        TRIM(city) AS clean_city
    FROM suppliers
    WHERE city IS NOT NULL
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating NULLS LAST, id) AS row_num
    FROM base
    QUALIFY row_num = 1
),
with_windows AS (
    SELECT
        *,
        LEAD(rating) OVER (PARTITION BY country ORDER BY row_num, id) AS next_rating,
        LAG(rating) OVER (PARTITION BY country ORDER BY row_num, id) AS prev_rating,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY row_num, id) AS row_seq
    FROM latest
),
kept_keys AS (
    SELECT id
    FROM with_windows
    INTERSECT ALL
    SELECT id
    FROM suppliers
    WHERE city IN ('London', 'Bangkok', 'Chiang Mai')
),
verified AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    CASE WHEN row_seq >= 8 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
    CASE WHEN rating >= 3.7 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    COUNT(DISTINCT country) AS distinct_countries,
    COUNT(*) FILTER (WHERE country = 'DE') AS de_rows,
    APPROX_PERCENTILE(next_rating, 0.9) AS next_rating_p90,
    COUNT_IF(country = 'SG') AS sg_events,
    COUNT(DISTINCT city) AS distinct_cities,
    COUNT_IF(country = 'TH') AS th_events
FROM verified
GROUP BY CUBE (CASE WHEN row_seq >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN rating >= 3.7 THEN 'HIGH' ELSE 'LOW' END)
HAVING COUNT_IF(country = 'SG') < 1000
ORDER BY de_rows NULLS LAST
