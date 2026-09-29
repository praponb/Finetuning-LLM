SELECT
    id,
    store_id,
    product_id,
    MIN(quantity) OVER w_grp AS last3_min_quantity,
    MAX(quantity) OVER w_grp AS last3_max_quantity,
    SUM(quantity) OVER w_grp AS last3_sum_quantity,
    COUNT(quantity) OVER w_grp AS last3_count_quantity
FROM inventory
WHERE quantity > 186
WINDOW w_grp AS (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
