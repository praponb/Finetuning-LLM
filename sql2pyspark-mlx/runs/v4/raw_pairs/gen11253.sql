SELECT id, rating, city, country, CONCAT_WS(',', country, city) country_city_key FROM suppliers ORDER BY rating DESC;
