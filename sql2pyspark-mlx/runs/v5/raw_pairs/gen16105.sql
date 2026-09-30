SELECT id, city FROM suppliers WHERE country = 'US' EXCEPT SELECT id, city FROM suppliers WHERE supplier_name LIKE 'Lio%';
