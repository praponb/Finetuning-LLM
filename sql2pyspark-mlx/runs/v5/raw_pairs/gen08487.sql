SELECT id, supplier_name, city FROM suppliers WHERE supplier_name NOT LIKE 'Tok%' UNION SELECT id, supplier_name, city FROM suppliers WHERE city = 'New York' ORDER BY supplier_name DESC;
