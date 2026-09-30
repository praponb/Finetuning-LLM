SELECT signup_date
FROM customers
WHERE signup_date BETWEEN '2023-07-01' AND '2023-09-10'
UNION
SELECT delivered_date
FROM shipments
ORDER BY signup_date;
