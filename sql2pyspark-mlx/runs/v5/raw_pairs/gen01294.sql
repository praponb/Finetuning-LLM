SELECT * FROM (SELECT brand, category, cost FROM products) src PIVOT (SUM(cost) AS amount, COUNT(1) AS n FOR category IN ('Grocery', 'Electronics'));
