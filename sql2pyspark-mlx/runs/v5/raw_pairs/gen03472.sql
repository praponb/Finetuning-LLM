SELECT
    id,
    brand,
    cost,
    AVG(cost) OVER (PARTITION BY brand) AS brand_avg_cost
FROM products
ORDER BY brand;
