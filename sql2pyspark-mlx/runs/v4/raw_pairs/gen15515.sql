SELECT city FROM customers WHERE LENGTH(email) < 12 EXCEPT SELECT city FROM stores
