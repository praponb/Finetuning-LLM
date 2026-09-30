SELECT
    *,
    SUM(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
    LEAD(quantity, 1) OVER (ORDER BY last_restock, id) AS next_quantity
FROM inventory;
