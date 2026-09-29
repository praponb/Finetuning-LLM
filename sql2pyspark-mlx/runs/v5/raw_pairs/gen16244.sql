SELECT id, brand, price, RANK() OVER (PARTITION BY brand ORDER BY price DESC) AS price_rank FROM products;
