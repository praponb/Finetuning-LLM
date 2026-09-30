SELECT id, category, measure, value FROM (SELECT id, category, price, cost FROM products) src UNPIVOT INCLUDE NULLS (value FOR measure IN (price AS PRICE, cost AS COST));
