SELECT hire_date, 'employee' AS source FROM employees UNION ALL SELECT sale_date, 'store_sale' AS source FROM store_sales;
