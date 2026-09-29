SELECT product_id, MIN(revenue) AS min_revenue FROM store_sales WHERE revenue <= 3200 AND revenue >= 2000 AND store_id IN (1, 3, 4, 5) GROUP BY product_id HAVING MIN(revenue) >= 3300
