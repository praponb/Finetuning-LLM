SELECT
    id,
    brand,
    stock_qty,
    MAX(stock_qty) OVER (PARTITION BY brand) AS brand_max_stock_qty
FROM products
