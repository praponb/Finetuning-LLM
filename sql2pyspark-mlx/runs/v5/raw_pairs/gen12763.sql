SELECT
    store_id,
    DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month,
    product_id,
    MIN(units) AS min_units
FROM store_sales
WHERE product_id = 5
GROUP BY store_id, DATE_FORMAT(sale_date, 'yyyy-MM'), product_id
HAVING min_units > 35;
