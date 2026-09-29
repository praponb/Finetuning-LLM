SELECT id, product_id, quantity, ROUND(quantity, 1) AS quantity_rounded FROM inventory WHERE last_restock BETWEEN '2024-09-01' AND '2025-03-01' AND store_id = 5;
