SELECT id, rating, supplier_name, country FROM suppliers WHERE city IS NOT NULL AND city IN ('Chiang Mai', 'Tokyo');
