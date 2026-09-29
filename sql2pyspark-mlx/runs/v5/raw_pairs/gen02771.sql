SELECT *, SUBSTRING(store_name, 1, 2) AS store_name_prefix FROM stores WHERE sqft BETWEEN 12370 AND 13250 AND sqft < 7545 ORDER BY sqft DESC, id LIMIT 5;
