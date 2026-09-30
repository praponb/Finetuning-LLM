WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        AVG(sqft) OVER (PARTITION BY region) AS region_avg_sqft
    FROM stores
)
SELECT
    id,
    city,
    region,
    opened_date,
    sqft,
    cumulative_sqft,
    region_avg_sqft,
    CASE WHEN city RLIKE '^(Berlin|Bangkok)$' THEN 'GROUP_A' WHEN city RLIKE '^(Chiang Mai|New York)$' THEN 'CORE' ELSE 'PRIORITY' END AS city_zone
FROM with_windows
ORDER BY cumulative_sqft;
