SELECT id, tags, launch_date, LAST_DAY(launch_date) AS launch_date_month_end FROM products ORDER BY launch_date, id LIMIT 10;
