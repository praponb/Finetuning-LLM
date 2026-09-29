SELECT *, REPLACE(name, ' ', '_') AS name_slug, DAYOFWEEK(signup_date) AS signup_date_dayofweek
FROM customers
WHERE name LIKE 'Gra%' AND age <= 61;
