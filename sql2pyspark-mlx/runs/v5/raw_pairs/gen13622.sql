SELECT
    QUARTER(p.launch_date) AS launch_quarter,
    DATE_TRUNC('MONTH', p.launch_date) AS launch_month,
    COUNT(i.quantity) AS quantity_count,
    MAX(i.last_restock) AS last_last_restock
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
GROUP BY QUARTER(p.launch_date), DATE_TRUNC('MONTH', p.launch_date)
ORDER BY quantity_count DESC;
