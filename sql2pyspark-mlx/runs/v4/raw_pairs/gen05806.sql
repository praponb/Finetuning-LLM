SELECT category, launch_date
FROM products
WHERE product_name LIKE '%Make%'
UNION ALL
SELECT category, launch_date
FROM products
WHERE brand IN ('Umbrella', 'Initech', 'Stark')
