WITH filtered_stores AS (
    SELECT *
    FROM stores
    WHERE sqft = 11687 AND sqft = 15505
),
summary AS (
    SELECT CASE WHEN city IN ('Singapore', 'London') THEN 'core' ELSE 'other' END AS city_group, COUNT(*) AS cnt, ROUND((100 * SUM(CASE WHEN region = 'West' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_west
    FROM filtered_stores
    GROUP BY CASE WHEN city IN ('Singapore', 'London') THEN 'core' ELSE 'other' END
)
SELECT city_group, pct_west
FROM summary
