SELECT order_date FROM orders EXCEPT SELECT last_restock FROM inventory WHERE quantity >= 197
