SELECT
    *,
    AVG(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS quantity_avg_2_prior,
    NTILE(4) OVER (ORDER BY quantity DESC, id) AS quantity_bucket,
    RANK() OVER (PARTITION BY store_id ORDER BY quantity NULLS LAST) AS quantity_rank
FROM inventory;
