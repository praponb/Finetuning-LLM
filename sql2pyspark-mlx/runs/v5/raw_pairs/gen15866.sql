SELECT
    i.last_restock,
    p.stock_qty,
    p.cost,
    p.brand
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
WHERE p.product_name LIKE '%Hub%'
