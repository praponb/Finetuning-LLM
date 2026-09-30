SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock DESC, id) row_seq,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) event_seq,
    SUM(quantity) OVER (PARTITION BY product_id, store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_quantity
FROM inventory
