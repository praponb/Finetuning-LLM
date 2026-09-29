SELECT id, launch_date, category, DATE_TRUNC('year', launch_date) AS launch_date_period FROM products
