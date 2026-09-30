SELECT
    id,
    stock_qty,
    category,
    launch_date,
    price,
    RANK() OVER (ORDER BY stock_qty) AS stock_qty_rank,
    LEAD(price, 2) OVER (PARTITION BY category ORDER BY launch_date, id) AS next_price
FROM products;
