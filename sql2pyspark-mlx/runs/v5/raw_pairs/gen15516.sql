SELECT country, rating, city, CONCAT_WS('-', supplier_name, country) AS supplier_name_country_key, CONCAT(country, ' - ', city) AS country_city FROM suppliers WHERE city = 'Berlin'
