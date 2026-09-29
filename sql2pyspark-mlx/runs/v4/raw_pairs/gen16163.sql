SELECT status
FROM accounts
WHERE opened_date >= '2024-04-23'
INTERSECT
SELECT status
FROM shipments
ORDER BY status;
