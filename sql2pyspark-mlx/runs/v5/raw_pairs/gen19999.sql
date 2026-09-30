SELECT location
FROM departments
WHERE budget < 2190000
EXCEPT
SELECT city
FROM customers
WHERE name LIKE 'Ja%';
