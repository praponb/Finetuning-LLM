SELECT id, city, LENGTH(city) AS city_length FROM suppliers WHERE rating >= 2.5 AND LENGTH(supplier_name) > 11;
