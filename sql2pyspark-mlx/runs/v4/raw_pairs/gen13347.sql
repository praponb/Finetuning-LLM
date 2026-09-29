SELECT product_id, MIN(revenue) AS min_revenue FROM store_sales WHERE revenue BETWEEN 1100 AND 2900 AND MONTH(sale_date) IN (6, 8, 9) GROUP BY product_id HAVING min_revenue >= 2600 ORDER BY 1 DESC
