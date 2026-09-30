SELECT
    brand,
    stock_qty,
    launch_date,
    COALESCE(product_name, 'Unknown') AS product_name_filled,
    stock_qty + cost AS stock_qty_plus_cost
FROM products
WHERE price < 570
