SELECT category, num_products, min_launch_date
FROM (
    SELECT category, COUNT(*) AS num_products, MIN(launch_date) AS min_launch_date
    FROM products
    WHERE price >= 640
    GROUP BY category
) AS sub
WHERE num_products >= 9
ORDER BY min_launch_date, category
