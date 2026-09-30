SELECT
    i.id,
    i.last_restock,
    i.store_id,
    s.opened_date,
    p.cost,
    p.launch_date
FROM inventory i
FULL OUTER JOIN stores s ON s.id = i.store_id
RIGHT JOIN products p ON i.product_id = p.id
WHERE p.stock_qty = 105;
