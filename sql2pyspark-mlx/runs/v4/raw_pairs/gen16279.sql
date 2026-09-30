SELECT country, supplier_name FROM suppliers WHERE supplier_name LIKE 'Eur%' UNION ALL SELECT country, supplier_name FROM suppliers WHERE country <> 'US' ORDER BY supplier_name
