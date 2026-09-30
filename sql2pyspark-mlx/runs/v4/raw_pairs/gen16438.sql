SELECT
    s.sqft,
    i.product_id,
    i.last_restock,
    LAST_DAY(i.last_restock) AS last_restock_month_end
FROM stores s
LEFT JOIN inventory i ON i.store_id = s.id
WHERE s.sqft > 7498 AND i.last_restock > '2025-01-29'
ORDER BY i.product_id
