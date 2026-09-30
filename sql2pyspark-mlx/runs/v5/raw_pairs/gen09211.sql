SELECT
    id,
    product_name,
    brand,
    price,
    cost,
    launch_date,
    ROW_NUMBER() OVER (PARTITION BY brand, category ORDER BY stock_qty, id) AS row_seq,
    AVG(stock_qty) OVER (PARTITION BY brand ORDER BY launch_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS stock_qty_avg_6
FROM products
WHERE supplier_id IN (4, 24);
