SELECT city, country FROM customers WHERE LENGTH(name) < 7 EXCEPT SELECT city, country FROM suppliers WHERE country = 'TH' ORDER BY city;
