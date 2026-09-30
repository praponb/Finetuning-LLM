SELECT
    id,
    product_name,
    category,
    brand,
    stock_qty,
    tags,
    AVG(cost) OVER w_grp AS cost_avg_2_prior,
    CORR(stock_qty, price) OVER (PARTITION BY supplier_id, brand) AS corr_stock_qty_price
FROM products
WINDOW w_grp AS (ORDER BY stock_qty, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
