SELECT units FROM store_sales WHERE store_id IN (1, 2, 4, 6) INTERSECT SELECT units FROM store_sales WHERE units = 39 ORDER BY units
