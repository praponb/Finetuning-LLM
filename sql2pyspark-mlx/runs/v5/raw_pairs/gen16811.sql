SELECT *
FROM (
    SELECT category, SORT_ARRAY(COLLECT_SET(category)) AS category_list, COUNT(cost) AS cost_count
    FROM products
    WHERE launch_date BETWEEN '2023-07-01' AND '2023-10-19'
    GROUP BY category
) AS t
WHERE cost_count < 7
ORDER BY category
LIMIT 50
