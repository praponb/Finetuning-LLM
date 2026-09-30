SELECT country, city FROM customers INTERSECT SELECT country, city FROM suppliers WHERE city = 'Bangkok';
