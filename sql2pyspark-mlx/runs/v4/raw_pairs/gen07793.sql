SELECT
    id,
    last_restock,
    store_id,
    product_id,
    CASE WHEN quantity >= 244 THEN 'High' WHEN quantity >= 106 THEN 'Medium' ELSE 'Low' END AS quantity_band
FROM inventory
