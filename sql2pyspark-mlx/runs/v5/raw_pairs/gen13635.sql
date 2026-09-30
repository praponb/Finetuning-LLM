SELECT *, UPPER(city) AS city_upper, CASE WHEN country IN ('UK', 'DE') THEN 'Priority' ELSE 'Other' END AS country_group
FROM suppliers
WHERE country != 'DE';
