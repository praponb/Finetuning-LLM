SELECT * FROM (SELECT id, brand, price, cost FROM products) src UNPIVOT INCLUDE NULLS (value FOR metric IN (price AS PRICE_VAL, cost));
