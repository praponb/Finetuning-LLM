SELECT
    id,
    category,
    brand,
    price,
    cost,
    FIRST_VALUE(cost) OVER (PARTITION BY category, brand ORDER BY price, id) AS first_cost
FROM products
WHERE stock_qty != 311;
