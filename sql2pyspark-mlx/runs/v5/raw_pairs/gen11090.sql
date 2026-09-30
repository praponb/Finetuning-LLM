SELECT
    *,
    RANK() OVER w_part AS quantity_rank,
    AVG(quantity) OVER w_grp AS quantity_avg_6,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS row_seq,
    RANK() OVER w_hist AS quantity_rank_2,
    FIRST_VALUE(quantity, TRUE) OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity
FROM inventory
WINDOW w_part AS (ORDER BY quantity NULLS FIRST), w_grp AS (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY store_id, product_id ORDER BY quantity DESC);
