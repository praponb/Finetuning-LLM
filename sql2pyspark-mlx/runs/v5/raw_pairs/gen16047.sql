SELECT city, id
FROM suppliers
EXCEPT ALL
SELECT city, id
FROM suppliers
WHERE city = 'Chiang Mai';
