SELECT
    id,
    product_id,
    quantity,
    LAG(quantity, 2) OVER (ORDER BY quantity ASC, id ASC) AS prev_quantity,
    DENSE_RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC NULLS LAST) AS quantity_dense_rank,
    SUM(quantity) OVER (PARTITION BY store_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity
FROM inventory
WHERE last_restock BETWEEN '2023-07-25' AND '2025-07-14';
