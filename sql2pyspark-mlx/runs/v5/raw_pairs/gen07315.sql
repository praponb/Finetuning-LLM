SELECT
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    tags,
    LAG(cost) OVER (PARTITION BY brand ORDER BY price, id) prev_cost,
    LAG(stock_qty) OVER (PARTITION BY brand ORDER BY price, id) prev_stock_qty,
    ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price, id) event_seq
FROM products
