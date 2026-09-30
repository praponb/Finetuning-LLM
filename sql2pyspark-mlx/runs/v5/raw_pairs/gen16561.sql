SELECT
    id,
    sale_date,
    revenue,
    LEAD(revenue, 2) OVER (ORDER BY sale_date, id) AS next_revenue
FROM store_sales
WHERE units >= 20;
