SELECT
    id,
    product_id,
    units,
    YEAR(sale_date) sale_date_year,
    CASE WHEN units >= 44 THEN 'High' WHEN units >= 20 THEN 'Medium' ELSE 'Low' END units_band
FROM store_sales
WHERE sale_date <= '2023-08-01'
ORDER BY product_id DESC;
