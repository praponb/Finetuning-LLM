SELECT id, store_id, units, WEEKOFYEAR(sale_date) AS sale_date_weekofyear FROM store_sales WHERE NOT (store_id IN (1, 6) AND store_id IN (4, 5, 6)) ORDER BY sale_date_weekofyear;
