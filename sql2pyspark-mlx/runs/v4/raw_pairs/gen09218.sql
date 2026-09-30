SELECT *
FROM (
    SELECT category, CASE WHEN category IN ('Books', 'Fashion') THEN 'core' ELSE 'other' END AS category_group, ROUND(AVG(cost), 2) AS avg_cost
    FROM products
    WHERE brand NOT IN ('Umbrella', 'Initech')
    GROUP BY category, CASE WHEN category IN ('Books', 'Fashion') THEN 'core' ELSE 'other' END
) AS sub
WHERE avg_cost > 52
ORDER BY category_group
