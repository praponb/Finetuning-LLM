SELECT
    *,
    LEAD(quantity, 2) OVER w_part next_quantity,
    SUM(quantity) OVER w_recent cumulative_quantity,
    SUM(quantity) OVER w_recent running_quantity,
    DENSE_RANK() OVER (ORDER BY quantity DESC NULLS FIRST) quantity_dense_rank,
    NTILE(3) OVER (ORDER BY quantity, id) quantity_bucket
FROM inventory
WINDOW w_part AS (PARTITION BY store_id ORDER BY last_restock, id), w_recent AS (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
