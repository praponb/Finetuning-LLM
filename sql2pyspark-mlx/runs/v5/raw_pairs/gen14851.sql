SELECT
    id,
    product_name,
    category,
    cost,
    stock_qty,
    tags,
    AVG(price) OVER (PARTITION BY brand, supplier_id ORDER BY launch_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS price_avg_2,
    RANK() OVER (ORDER BY price) AS price_rank
FROM products;
