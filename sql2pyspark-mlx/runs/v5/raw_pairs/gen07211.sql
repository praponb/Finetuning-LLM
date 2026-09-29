SELECT opened_date
FROM accounts
INTERSECT
SELECT shipped_date
FROM shipments
WHERE carrier = 'UPS'
ORDER BY opened_date DESC;
