SELECT
    last_restock,
    store_id,
    CAST(quantity AS STRING) AS quantity_str,
    MONTH(last_restock) AS last_restock_month
FROM inventory
ORDER BY last_restock DESC;
