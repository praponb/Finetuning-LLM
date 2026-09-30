SELECT id, region
FROM stores
WHERE opened_date >= '2024-09-01'
UNION
SELECT id, region
FROM stores
WHERE region IN ('North', 'West', 'East')
ORDER BY region DESC;
