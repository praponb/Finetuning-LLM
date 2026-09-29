SELECT
    s.sqft,
    s.store_name,
    s.region,
    i.store_id
FROM stores s
LEFT JOIN inventory i ON s.id = i.store_id
WHERE i.last_restock BETWEEN '2025-02-06' AND '2025-08-14' AND i.last_restock > '2023-10-02'
ORDER BY i.store_id DESC;
