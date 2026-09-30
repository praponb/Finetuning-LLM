SELECT
    YEAR(launch_date) AS launch_year,
    supplier_id,
    MAX(launch_date) AS latest_launch_date,
    ROUND(STDDEV(stock_qty), 2) AS stddev_stock_qty
FROM products
GROUP BY launch_year, supplier_id
