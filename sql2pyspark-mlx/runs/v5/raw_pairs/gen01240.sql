SELECT opened_date
FROM accounts
INTERSECT
SELECT delivered_date AS opened_date
FROM shipments
ORDER BY opened_date;
