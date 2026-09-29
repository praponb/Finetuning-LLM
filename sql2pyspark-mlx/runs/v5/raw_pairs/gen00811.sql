SELECT store_id, ROUND(STDDEV(quantity), 2) stddev_quantity
FROM inventory
WHERE last_restock BETWEEN '2025-01-01' AND '2025-07-01' OR last_restock BETWEEN '2023-12-26' AND '2025-02-01'
GROUP BY store_id
ORDER BY store_id ASC;
