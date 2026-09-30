SELECT store_name, opened_date FROM stores WHERE region IS NULL UNION SELECT store_name, opened_date FROM stores WHERE sqft <> 9689 ORDER BY opened_date DESC
