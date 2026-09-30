SELECT
    id,
    category,
    launch_date,
    stock_qty,
    brand,
    price,
    SUM(stock_qty) OVER (PARTITION BY category ORDER BY launch_date, id) AS running_stock_qty,
    ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price, id) AS row_num
FROM products
