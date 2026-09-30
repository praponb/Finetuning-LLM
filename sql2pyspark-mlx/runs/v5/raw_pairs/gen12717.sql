SELECT
    DATE_TRUNC('MONTH', launch_date) AS launch_month,
    category,
    brand,
    MAX(price) AS max_price
FROM products
WHERE category NOT IN ('Home', 'Sports', 'Grocery')
GROUP BY DATE_TRUNC('MONTH', launch_date), category, brand
HAVING MAX(price) > 720
