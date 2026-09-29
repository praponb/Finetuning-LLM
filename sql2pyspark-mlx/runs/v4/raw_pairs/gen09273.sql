SELECT revenue, sale_date, revenue * units AS revenue_per_unit FROM store_sales WHERE product_id IN (1, 6) AND store_id IN (2, 3, 4, 5) AND units BETWEEN 17 AND 33 ORDER BY sale_date;
