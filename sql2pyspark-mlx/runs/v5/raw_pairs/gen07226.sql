SELECT id, sale_date, revenue, revenue - LAG(revenue, 1, 0) OVER (ORDER BY sale_date, id) AS revenue_change FROM store_sales WHERE units >= 16;
