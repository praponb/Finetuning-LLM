SELECT
    id,
    price,
    cost,
    tags,
    DENSE_RANK() OVER (ORDER BY cost DESC NULLS LAST) AS cost_dense_rank,
    AVG(price) OVER w_running AS price_avg_5_prior,
    ROW_NUMBER() OVER w_hist AS event_seq,
    DENSE_RANK() OVER (ORDER BY stock_qty DESC) AS stock_qty_dense_rank
FROM products
WINDOW w_running AS (PARTITION BY supplier_id ORDER BY price, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_hist AS (PARTITION BY category, brand ORDER BY launch_date DESC, id);
