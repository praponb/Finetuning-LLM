SELECT
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC) AS quantity_rank,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock ASC, id ASC) AS row_seq,
    ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC, id ASC) AS row_seq_2,
    SUM(quantity) OVER (PARTITION BY product_id ORDER BY last_restock ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity
FROM inventory;
