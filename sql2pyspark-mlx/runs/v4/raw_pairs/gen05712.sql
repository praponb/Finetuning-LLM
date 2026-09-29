SELECT id, opened_date, region
FROM stores
WHERE city = 'Berlin'
UNION ALL
SELECT id, opened_date, region
FROM stores
WHERE opened_date < '2025-08-01'
ORDER BY region;
