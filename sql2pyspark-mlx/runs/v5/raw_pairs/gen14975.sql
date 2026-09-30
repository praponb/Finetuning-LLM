SELECT
    s.city,
    i.store_id,
    i.last_restock,
    i.quantity
FROM stores s
INNER JOIN inventory i ON i.store_id = s.id
WHERE MONTH(s.opened_date) IN (2, 3, 5) AND YEAR(s.opened_date) >= 2024;
