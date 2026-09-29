SELECT sale_month, avg_revenue, revenue_count
FROM (
    SELECT DATE_FORMAT(sale_date, 'yyyy-MM') sale_month, AVG(revenue) avg_revenue, COUNT(revenue) revenue_count
    FROM store_sales
    GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM')
) s
WHERE revenue_count >= 4
