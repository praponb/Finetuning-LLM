SELECT sale_date, units, DATE_FORMAT(sale_date, 'yyyy-MM-dd') sale_date_label FROM store_sales ORDER BY sale_date_label DESC;
