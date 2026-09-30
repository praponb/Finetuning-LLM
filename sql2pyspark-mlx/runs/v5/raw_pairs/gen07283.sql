SELECT
    i.id,
    i.quantity,
    i.last_restock,
    i.store_id,
    s.region,
    DATEDIFF(CURRENT_DATE(), s.opened_date) AS days_since_opened_date
FROM inventory i
FULL OUTER JOIN stores s ON i.store_id = s.id
WHERE i.product_id IN (1, 2, 9)
ORDER BY i.last_restock ASC, i.quantity ASC;
