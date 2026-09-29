SELECT
    brand,
    SUM(CASE WHEN category = 'Home' THEN stock_qty ELSE 0 END) AS home_stock_qty,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    MAX(stock_qty) AS max_stock_qty
FROM products
GROUP BY brand
ORDER BY 4 ASC
