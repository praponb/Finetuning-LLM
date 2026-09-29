SELECT
    *,
    PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) AS quantity_percent_rank,
    AVG(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS quantity_avg_3
FROM inventory;
