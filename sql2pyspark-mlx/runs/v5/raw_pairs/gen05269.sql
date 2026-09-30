SELECT id, units, store_id FROM store_sales WHERE units > (SELECT MAX(units) FROM store_sales) ORDER BY id DESC, store_id
