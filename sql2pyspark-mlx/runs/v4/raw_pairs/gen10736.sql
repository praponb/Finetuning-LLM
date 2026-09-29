SELECT id, region, opened_date FROM stores WHERE LENGTH(store_name) > 11 UNION SELECT id, region, opened_date FROM stores WHERE sqft >= 4404;
