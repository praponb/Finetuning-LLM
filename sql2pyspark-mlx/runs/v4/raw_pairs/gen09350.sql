SELECT
    id,
    store_id,
    sale_date,
    revenue,
    ADD_MONTHS(sale_date, 12) AS sale_date_plus_months
FROM store_sales
WHERE revenue IS NOT NULL
ORDER BY sale_date ASC;
