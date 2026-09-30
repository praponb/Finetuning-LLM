SELECT
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    stock_qty,
    launch_date,
    NTILE(4) OVER (PARTITION BY category, brand ORDER BY stock_qty DESC, id) AS stock_qty_bucket,
    LEAD(price, 2) OVER (PARTITION BY brand ORDER BY stock_qty, id) AS next_price,
    CORR(stock_qty, price) OVER (PARTITION BY supplier_id) AS corr_stock_qty_price,
    SUM(stock_qty) OVER (PARTITION BY supplier_id ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_stock_qty
FROM products;
