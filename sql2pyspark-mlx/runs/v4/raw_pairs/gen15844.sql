SELECT
    i.id,
    i.store_id,
    i.last_restock,
    p.cost,
    s.region,
    s.opened_date
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON i.store_id = s.id
WHERE LENGTH(p.product_name) >= 8 AND s.region <> 'North'
ORDER BY id DESC, last_restock;
