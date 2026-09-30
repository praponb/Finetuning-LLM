SELECT
    id,
    sale_date,
    revenue,
    YEAR(sale_date) AS sale_date_year,
    DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
FROM store_sales
ORDER BY revenue, id
LIMIT 5;
