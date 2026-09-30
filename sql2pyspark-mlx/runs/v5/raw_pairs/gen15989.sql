SELECT id, opened_date, city, sqft FROM stores WHERE opened_date = (SELECT MIN(opened_date) FROM stores WHERE store_name IS NULL);
