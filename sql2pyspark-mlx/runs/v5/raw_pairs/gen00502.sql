SELECT
    category,
    supplier_id,
    MONTH(launch_date) AS launch_month,
    MAX(stock_qty) AS max_stock_qty
FROM products
GROUP BY category, supplier_id, MONTH(launch_date)
HAVING MAX(stock_qty) < 190;
