SELECT
    id,
    supplier_id,
    stock_qty,
    brand,
    launch_date,
    ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY stock_qty, id) AS stock_qty_row,
    RANK() OVER (PARTITION BY brand ORDER BY launch_date) AS launch_date_rank
FROM products
