WITH Ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY price DESC, id) AS pos FROM products) SELECT id, supplier_id, price FROM Ranked WHERE pos <= 5;
