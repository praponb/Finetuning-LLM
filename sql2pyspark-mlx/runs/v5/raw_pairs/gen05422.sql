SELECT
    id,
    product_name,
    CONCAT(product_name, ', ', category) AS product_name_category,
    CASE WHEN category IN ('Books', 'Sports') THEN 'Tier 1' ELSE 'Other' END AS category_group
FROM products
WHERE MONTH(launch_date) IN (2, 3, 6)
ORDER BY category_group, id
LIMIT 10;
