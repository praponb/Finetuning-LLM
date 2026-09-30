WITH filtered_stores AS (
    SELECT *
    FROM stores
    WHERE YEAR(opened_date) >= 2023 AND opened_date BETWEEN '2023-10-01' AND '2025-01-01'
),
stores_stats AS (
    SELECT city, ROUND((100 * SUM(CASE WHEN region = 'West' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_west
    FROM filtered_stores
    GROUP BY city
)
SELECT city, pct_west
FROM stores_stats
ORDER BY 2 DESC
