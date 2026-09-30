SELECT id, supplier_name FROM suppliers WHERE rating <= 4.1 EXCEPT SELECT id, supplier_name FROM suppliers WHERE supplier_name IS NOT NULL
