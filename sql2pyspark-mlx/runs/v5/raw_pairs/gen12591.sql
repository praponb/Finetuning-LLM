SELECT
    category,
    brand,
    latest_launch_date,
    avg_cost
FROM (
    SELECT
        category,
        brand,
        MAX(launch_date) AS latest_launch_date,
        AVG(cost) AS avg_cost
    FROM products
    GROUP BY category, brand
) AS x
WHERE avg_cost < 540
ORDER BY brand, category
LIMIT 50;
