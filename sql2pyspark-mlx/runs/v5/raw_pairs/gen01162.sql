SELECT
    id,
    units,
    store_id,
    YEAR(sale_date) AS sale_date_year
FROM store_sales
WHERE revenue < 1700
ORDER BY 2, id
LIMIT 20
