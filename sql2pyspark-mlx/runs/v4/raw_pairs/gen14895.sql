SELECT id, store_id, units FROM store_sales WHERE units > 18 UNION ALL SELECT id, store_id, units FROM store_sales WHERE units <= 17 ORDER BY id
