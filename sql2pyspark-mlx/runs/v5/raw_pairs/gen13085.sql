SELECT
    id,
    supplier_id,
    launch_date,
    stock_qty,
    stock_qty - LAG(stock_qty, 2) OVER (PARTITION BY supplier_id ORDER BY launch_date, id) AS stock_qty_change
FROM products
WHERE ARRAY_CONTAINS(tags, 'new');
