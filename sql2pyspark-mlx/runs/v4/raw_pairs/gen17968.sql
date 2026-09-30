SELECT store_name, UPPER(city) AS city_upper
FROM stores
ORDER BY city_upper DESC;
