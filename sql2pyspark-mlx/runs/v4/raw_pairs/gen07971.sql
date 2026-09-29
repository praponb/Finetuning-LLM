SELECT id, units, revenue, store_id FROM store_sales WHERE units >= (SELECT MAX(units) FROM store_sales WHERE store_id IN (1, 2, 4, 6));
