SELECT DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, MAX(sale_date) AS latest_sale_date, MAX(units) AS max_units FROM store_sales GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM');
