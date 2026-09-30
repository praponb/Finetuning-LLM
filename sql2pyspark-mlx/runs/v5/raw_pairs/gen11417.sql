SELECT *, TRIM(country) AS clean_country
FROM suppliers
WHERE rating > 4.1;
