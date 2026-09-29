SELECT
    id,
    stock_qty,
    supplier_id,
    MONTH(launch_date) AS launch_date_month
FROM products;
