SELECT last_restock, quantity FROM inventory WHERE last_restock BETWEEN '2023-09-06' AND '2024-03-01' AND quantity != 163 ORDER BY last_restock DESC
