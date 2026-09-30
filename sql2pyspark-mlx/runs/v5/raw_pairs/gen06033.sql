SELECT opened_date
FROM stores
WHERE city = 'Berlin'
UNION ALL
SELECT opened_date
FROM stores
WHERE LENGTH(store_name) < 9;
