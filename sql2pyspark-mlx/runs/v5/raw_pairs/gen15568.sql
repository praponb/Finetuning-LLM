SELECT id, country, rating, CONCAT(supplier_name, '/', country) supplier_name_country FROM suppliers WHERE (supplier_name IS NOT NULL AND rating < 2.3) OR city = 'New York'
