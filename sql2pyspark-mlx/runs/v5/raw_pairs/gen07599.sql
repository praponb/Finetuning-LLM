SELECT
    store_id,
    revenue,
    DATE_FORMAT(sale_date, 'yyyy-MM-dd') AS sale_date_label,
    ROUND(units * 1.07, 2) AS units_adjusted
FROM store_sales
WHERE product_id IN (2, 4, 7, 8);
