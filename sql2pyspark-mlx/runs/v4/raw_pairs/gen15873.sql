SELECT *, DATE_TRUNC('WEEK', sale_date) AS sale_date_period FROM store_sales WHERE (revenue BETWEEN 1800 AND 2700 AND store_id IN (3, 5)) OR product_id IN (4, 5);
