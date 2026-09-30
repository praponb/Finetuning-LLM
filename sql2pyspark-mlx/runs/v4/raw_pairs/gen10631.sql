SELECT *, SUBSTRING(supplier_name, 1, 2) supplier_name_prefix, LENGTH(country) country_length FROM suppliers WHERE rating BETWEEN 3.3 AND 4.0 ORDER BY supplier_name DESC;
