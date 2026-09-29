WITH Numbered AS (SELECT id, brand, price, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price DESC, id) AS rn FROM products) SELECT id, brand, price FROM Numbered WHERE rn <= 5 ORDER BY price DESC;
