SELECT
    id,
    units,
    revenue,
    DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
FROM store_sales
WHERE units > 30
ORDER BY id DESC
LIMIT 5;
