SELECT
    id,
    category,
    brand,
    cost,
    stock_qty,
    launch_date,
    tags,
    supplier_id,
    FIRST_VALUE(cost, TRUE) OVER (PARTITION BY category ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost,
    LAST_VALUE(brand) IGNORE NULLS OVER (PARTITION BY supplier_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_brand,
    AVG(stock_qty) OVER (ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS stock_qty_avg_2,
    AVG(price) OVER (PARTITION BY supplier_id ORDER BY price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS price_avg_6_prior
FROM products
WHERE category IN ('Grocery', 'Electronics');
