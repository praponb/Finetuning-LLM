WITH eligible AS (
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.order_date <= s.opened_date
        )
)
SELECT
    CASE WHEN sqft >= 15918 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
    city,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN city = 'London' THEN 1 END), 0), 2) AS sqft_per_london,
    APPROX_PERCENTILE(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles
FROM eligible
GROUP BY CASE WHEN sqft >= 15918 THEN 'HIGH' ELSE 'LOW' END, city
HAVING avg_sqft < 1000000
ORDER BY city DESC
