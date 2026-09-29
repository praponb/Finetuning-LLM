SELECT
    id,
    category,
    launch_date,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date, id) AS launch_date_row
FROM products
ORDER BY id DESC;
