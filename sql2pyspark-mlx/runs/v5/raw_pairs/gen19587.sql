SELECT id, sqft
FROM stores
WHERE sqft BETWEEN 8146 AND 10353
UNION
SELECT id, sqft
FROM stores
WHERE YEAR(opened_date) = 2023
