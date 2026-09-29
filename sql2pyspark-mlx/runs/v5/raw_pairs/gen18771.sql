SELECT id, city FROM suppliers WHERE LOWER(supplier_name) LIKE '%lob%' EXCEPT SELECT id, city FROM suppliers WHERE city <> 'New York'
