select rating, concat_ws('/', country, city) as country_city_key, concat(supplier_name, '/', city) as supplier_name_city from suppliers where city = 'New York' and city = 'Singapore'
