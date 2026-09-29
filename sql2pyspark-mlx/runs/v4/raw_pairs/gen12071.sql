SELECT id, quantity, LAST_DAY(last_restock) AS last_restock_month_end
FROM inventory
WHERE last_restock BETWEEN '2023-06-28' AND '2024-12-01' AND last_restock BETWEEN '2023-04-01' AND '2025-05-28';
