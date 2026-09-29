SELECT product_id, ROUND(AVG(revenue), 2) AS avg_revenue FROM store_sales WHERE (product_id = 7 AND revenue >= 2700) OR revenue > 3900 GROUP BY product_id ORDER BY product_id ASC, avg_revenue DESC;
