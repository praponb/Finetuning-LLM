SELECT
    QUARTER(launch_date) AS launch_quarter,
    category,
    MAX(price) AS max_price,
    SUM(CASE WHEN category = 'Grocery' THEN stock_qty ELSE 0 END) AS grocery_stock_qty,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list
FROM products
WHERE product_name NOT LIKE '%Bac%' AND (ARRAY_CONTAINS(tags, 'sale') OR price <= 370)
GROUP BY CUBE(QUARTER(launch_date), category)
ORDER BY launch_quarter
