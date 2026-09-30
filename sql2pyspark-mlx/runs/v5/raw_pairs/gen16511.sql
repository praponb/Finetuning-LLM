SELECT region, city
FROM stores
EXCEPT DISTINCT
SELECT region, city
FROM stores
WHERE opened_date BETWEEN '2023-05-01' AND '2023-08-01';
