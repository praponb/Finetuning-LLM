SELECT sale_date FROM store_sales WHERE units = 35 EXCEPT SELECT last_restock sale_date FROM inventory
