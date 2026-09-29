SELECT id, store_id, revenue + units revenue_per_unit, DATE_FORMAT(sale_date, 'yyyy-MM') sale_date_label FROM store_sales;
