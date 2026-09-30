SELECT units FROM store_sales WHERE product_id IN (1, 2, 3) INTERSECT SELECT units FROM store_sales WHERE store_id IN (4, 6) ORDER BY units DESC;
