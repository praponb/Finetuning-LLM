SELECT category, SUM(cost) AS total_cost, DENSE_RANK() OVER (ORDER BY SUM(cost) DESC) AS rnk FROM products WHERE price > 580 GROUP BY category;
