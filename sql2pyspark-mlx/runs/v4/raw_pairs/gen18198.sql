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
    SUM(price) OVER (PARTITION BY supplier_id, brand) AS supplier_id_sum_price,
    AVG(cost) OVER w_ordered AS cost_avg_2
FROM products
WINDOW w_ordered AS (PARTITION BY supplier_id ORDER BY price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
