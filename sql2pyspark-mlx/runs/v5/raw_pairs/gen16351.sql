SELECT store_name, sqft FROM stores WHERE store_name IS NOT NULL EXCEPT SELECT store_name, sqft FROM stores WHERE city != 'Tokyo'
