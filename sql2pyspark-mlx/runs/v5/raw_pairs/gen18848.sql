SELECT brand, stddev_cost, num_products
FROM (
    SELECT brand, ROUND(STDDEV(cost), 2) AS stddev_cost, COUNT(*) AS num_products
    FROM products
    GROUP BY brand
) AS sub
WHERE stddev_cost > 33
ORDER BY stddev_cost DESC;
