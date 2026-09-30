SELECT
    id,
    brand,
    launch_date,
    cost,
    price,
    LAG(cost, 1, 0) OVER (PARTITION BY brand ORDER BY launch_date, id) AS prev_cost,
    ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price DESC, id) AS rn
FROM products
ORDER BY prev_cost;
