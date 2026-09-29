SELECT *, DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_date_label, UPPER(category) AS category_upper FROM products ORDER BY id DESC, launch_date_label;
