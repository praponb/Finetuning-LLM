SELECT
    id,
    launch_date,
    price,
    category,
    cost,
    price - LAG(price, 2) OVER (ORDER BY launch_date, id) AS price_change,
    AVG(cost) OVER (PARTITION BY category) AS category_avg_cost
FROM products;
