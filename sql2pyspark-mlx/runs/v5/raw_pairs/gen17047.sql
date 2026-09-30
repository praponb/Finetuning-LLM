SELECT product_id, avg_revenue FROM (SELECT product_id, ROUND(AVG(revenue), 2) avg_revenue FROM store_sales GROUP BY product_id) x WHERE avg_revenue > 63
