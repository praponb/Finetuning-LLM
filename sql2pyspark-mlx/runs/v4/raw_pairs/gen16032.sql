SELECT
    id,
    price,
    product_name,
    LAST_DAY(launch_date) AS launch_date_month_end
FROM products
ORDER BY product_name;
