SELECT store_id, product_id FROM inventory EXCEPT SELECT store_id, product_id FROM inventory WHERE YEAR(last_restock) >= 2023;
