WITH remaining_keys AS (
    SELECT id
    FROM stores
    INTERSECT
    SELECT id
    FROM stores
    WHERE city IN ('Berlin', 'New York', 'London')
),
verified AS (
    SELECT s.*
    FROM stores s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    city,
    PERCENTILE_APPROX(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles,
    COUNT(*) AS row_count,
    COUNT(*) FILTER (WHERE region = 'South') AS south_rows,
    COUNT(*) FILTER (WHERE city = 'London') AS london_rows,
    COUNT(DISTINCT region) AS distinct_regions,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM verified
GROUP BY CUBE (DATE_TRUNC('MONTH', opened_date), city)
HAVING london_rows < 1
ORDER BY num_cities DESC NULLS LAST;
