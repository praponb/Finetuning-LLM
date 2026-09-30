SELECT
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    stock_qty,
    launch_date,
    tags,
    supplier_id,
    SUM(cost) OVER (PARTITION BY category, brand ORDER BY launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
    STDDEV_SAMP(price) OVER (PARTITION BY supplier_id) AS supplier_id_stddev_samp_price,
    LAG(stock_qty, 1, 0) OVER (PARTITION BY brand ORDER BY launch_date ASC, id ASC) AS prev_stock_qty,
    COUNT(cost) OVER (PARTITION BY brand) AS brand_count_cost,
    LEAD(cost, 1, 0) OVER (PARTITION BY supplier_id, category ORDER BY stock_qty ASC, id ASC) AS next_cost
FROM products
