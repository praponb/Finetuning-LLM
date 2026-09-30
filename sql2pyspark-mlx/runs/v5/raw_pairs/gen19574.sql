SELECT last_restock FROM inventory WHERE store_id IN (3, 4, 5) EXCEPT SELECT last_restock FROM inventory WHERE YEAR(last_restock) >= 2023;
