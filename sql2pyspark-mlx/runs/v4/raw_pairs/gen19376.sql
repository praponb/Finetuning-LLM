SELECT id, store_id, DATE_TRUNC('MONTH', sale_date) AS sale_date_period FROM store_sales WHERE sale_date >= '2024-07-01';
