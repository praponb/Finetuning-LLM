SELECT
    o.store_id,
    s.city,
    s.opened_date,
    s.store_name,
    ROUND(o.discount / o.amount, 2) AS discount_per_amount
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
WHERE s.city != 'New York'
ORDER BY s.city, store_name;
