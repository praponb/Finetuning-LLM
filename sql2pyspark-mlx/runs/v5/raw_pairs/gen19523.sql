SELECT
    id,
    category,
    supplier_id,
    launch_date,
    cost,
    brand,
    stock_qty,
    SUM(cost) OVER (PARTITION BY category, supplier_id ORDER BY launch_date, id) AS running_cost,
    LEAD(stock_qty, 1, 0) OVER (PARTITION BY brand ORDER BY launch_date, id) AS next_stock_qty
FROM products
WHERE price > 980;
