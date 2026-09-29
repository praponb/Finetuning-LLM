SELECT *, WEEKOFYEAR(last_restock) AS last_restock_weekofyear, CEIL(quantity) AS quantity_whole
FROM inventory
WHERE (last_restock <= '2024-10-06' AND store_id IN (2, 3)) OR last_restock <= '2025-05-15'
