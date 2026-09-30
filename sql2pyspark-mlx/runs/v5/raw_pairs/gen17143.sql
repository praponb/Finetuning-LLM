SELECT id, store_name
FROM stores
WHERE sqft >= 8618
UNION
SELECT id, store_name
FROM stores
WHERE opened_date >= '2025-09-01'
