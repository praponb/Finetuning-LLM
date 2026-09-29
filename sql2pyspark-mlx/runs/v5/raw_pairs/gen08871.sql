SELECT
    product_id,
    last_restock,
    LAST_DAY(last_restock) AS last_restock_month_end,
    LAST_DAY(last_restock) AS last_restock_month_end_2
FROM inventory
WHERE last_restock BETWEEN '2023-05-01' AND '2025-01-01' AND product_id IN (2, 5)
ORDER BY last_restock_month_end_2
