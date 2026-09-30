SELECT store_name, INITCAP(TRIM(store_name)) AS store_name_title FROM stores WHERE NOT (sqft <= 4566 AND sqft > 9482);
