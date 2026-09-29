SELECT
    i.id,
    i.quantity,
    s.opened_date,
    p.cost
FROM inventory i
LEFT OUTER JOIN stores s ON i.store_id = s.id
JOIN products p ON i.product_id = p.id
ORDER BY 1 DESC
