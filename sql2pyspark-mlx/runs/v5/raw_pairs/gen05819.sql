SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) AS pos FROM inventory) AS r WHERE pos <= 3;
