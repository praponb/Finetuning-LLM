SELECT * FROM (SELECT id, category, price, brand, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC, id) AS rnk FROM products) AS r WHERE rnk = 1;
