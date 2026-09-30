SELECT country, city FROM customers EXCEPT SELECT country, city FROM suppliers WHERE rating <= 3.0
