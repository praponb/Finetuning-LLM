WITH recent_customers AS (
    SELECT *
    FROM customers
    WHERE city IN ('Berlin', 'London', 'Tokyo') AND segment = 'SMB' AND city != 'New York'
),
customers_stats AS (
    SELECT CASE WHEN city IN ('New York', 'Berlin') THEN 'tier_1' ELSE 'other' END AS city_group, SUM(CASE WHEN country = 'JP' THEN age ELSE 0 END) AS jp_age
    FROM recent_customers
    GROUP BY CASE WHEN city IN ('New York', 'Berlin') THEN 'tier_1' ELSE 'other' END
)
SELECT *
FROM customers_stats
ORDER BY jp_age, city_group;
