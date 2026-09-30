SELECT
    MONTH(p.launch_date) AS launch_month,
    p.brand,
    MIN(ss.units) AS min_units,
    AVG(ss.units) AS avg_units
FROM store_sales ss
JOIN products p ON p.id = ss.product_id
RIGHT JOIN suppliers s ON p.supplier_id = s.id
WHERE UPPER(TRIM(p.product_name)) = 'LAPTOP PRO 14'
GROUP BY MONTH(p.launch_date), p.brand
ORDER BY brand DESC, launch_month ASC
LIMIT 1;
