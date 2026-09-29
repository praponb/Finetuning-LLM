SELECT
    id,
    product_id,
    last_restock,
    RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC) rnk,
    NTILE(3) OVER (PARTITION BY product_id ORDER BY last_restock, id) last_restock_ntile
FROM inventory
WHERE MONTH(last_restock) IN (1, 6, 10)
ORDER BY 5
