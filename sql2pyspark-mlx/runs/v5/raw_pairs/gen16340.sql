SELECT opened_date FROM stores WHERE UPPER(TRIM(store_name)) = 'OLD TOWN' UNION ALL SELECT sale_date FROM store_sales ORDER BY opened_date DESC;
