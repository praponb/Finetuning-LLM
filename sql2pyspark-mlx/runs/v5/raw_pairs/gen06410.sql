SELECT id, category, cost, MIN(cost) OVER (PARTITION BY category) AS category_min_cost FROM products;
