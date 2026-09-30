SELECT id, brand, cost, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost, id) AS rn FROM products WHERE cost BETWEEN 340 AND 690;
