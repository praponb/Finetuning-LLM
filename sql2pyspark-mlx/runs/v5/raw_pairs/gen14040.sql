SELECT id, store_id, quantity FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS pos FROM inventory) AS r WHERE pos = 1
