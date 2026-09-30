SELECT
    product_id,
    store_id,
    last_restock,
    LAST_DAY(last_restock) AS last_restock_month_end
FROM inventory
WHERE product_id = 1 AND last_restock BETWEEN '2023-09-01' AND '2025-08-30';
