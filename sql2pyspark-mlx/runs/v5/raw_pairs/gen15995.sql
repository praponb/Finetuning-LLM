SELECT id, store_id, sale_date, revenue FROM (SELECT id, store_id, sale_date, revenue, RANK() OVER (PARTITION BY store_id ORDER BY sale_date) AS pos FROM store_sales) AS ranked WHERE pos <= 3
