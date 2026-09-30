SELECT
    brand,
    SUM(cost) AS total_cost,
    COUNT(*) AS cnt,
    RANK() OVER (ORDER BY SUM(cost) DESC) AS rnk
FROM products
GROUP BY brand
ORDER BY 3, brand
LIMIT 10
