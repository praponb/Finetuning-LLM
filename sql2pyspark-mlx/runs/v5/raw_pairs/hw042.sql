SELECT id, category, price, RANK() OVER (PARTITION BY category ORDER BY price DESC) AS price_rank FROM products;
