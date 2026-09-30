WITH kept_keys AS (
    SELECT id, city
    FROM stores
    EXCEPT DISTINCT
    SELECT id, city
    FROM stores
    WHERE city = 'New York'
),
reconciled AS (
    SELECT s.*
    FROM stores s
    INNER JOIN kept_keys k ON s.id = k.id AND s.city = k.city
)
SELECT
    CASE WHEN sqft >= 15522 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_tier,
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    PERCENTILE_APPROX(sqft, 0.5) AS sqft_p50,
    COUNT(CASE WHEN region = 'East' THEN 1 END) AS east_count,
    AVG(sqft) AS avg_sqft
FROM reconciled
GROUP BY GROUPING SETS ((CASE WHEN sqft >= 15522 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', opened_date)), (CASE WHEN sqft >= 15522 THEN 'PREMIUM' ELSE 'STANDARD' END))
HAVING COUNT(CASE WHEN region = 'East' THEN 1 END) >= 1
ORDER BY sqft_p50 DESC;
