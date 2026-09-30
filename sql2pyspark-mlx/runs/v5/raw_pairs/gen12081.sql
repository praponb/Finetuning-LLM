SELECT QUARTER(sale_date) AS sale_quarter, SUM(revenue) AS total_revenue, MAX(revenue) - MIN(revenue) AS revenue_range
FROM store_sales
GROUP BY 1;
