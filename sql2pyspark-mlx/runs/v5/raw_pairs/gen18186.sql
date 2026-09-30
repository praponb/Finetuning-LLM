SELECT p.brand, MONTH(p.launch_date) AS launch_month, MAX(i.last_restock) AS last_last_restock
FROM inventory i
LEFT OUTER JOIN products p ON i.product_id = p.id
GROUP BY p.brand, MONTH(p.launch_date)
ORDER BY last_last_restock DESC;
