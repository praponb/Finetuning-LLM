SELECT
    id,
    brand,
    price,
    cost,
    stock_qty,
    launch_date,
    NTILE(3) OVER (PARTITION BY brand ORDER BY cost DESC, id) AS cost_bucket,
    RANK() OVER w_grp AS stock_qty_rank,
    AVG(cost) OVER (PARTITION BY supplier_id) AS supplier_id_avg_cost
FROM products
WINDOW w_grp AS (PARTITION BY category ORDER BY stock_qty DESC)
