SELECT id, supplier_name, country FROM suppliers WHERE country IN ('JP', 'DE', 'US') EXCEPT SELECT id, supplier_name, country FROM suppliers WHERE city IN ('London', 'New York', 'Chiang Mai')
