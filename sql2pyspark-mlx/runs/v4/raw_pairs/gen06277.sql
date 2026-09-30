SELECT *, AVG(revenue) OVER (PARTITION BY store_id) AS store_id_avg_revenue, AVG(revenue) OVER (ORDER BY revenue, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS revenue_avg_5 FROM store_sales
