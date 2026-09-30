SELECT *, UPPER(city) AS city_upper, LOWER(city) AS city_lower
FROM suppliers
WHERE supplier_name IS NOT NULL AND (country NOT IN ('DE', 'SG', 'TH') OR city NOT IN ('London', 'Berlin'))
ORDER BY rating, city_lower;
