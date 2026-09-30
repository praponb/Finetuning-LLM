SELECT opened_date
FROM accounts
WHERE customer_id IS NULL
UNION ALL
SELECT launch_date AS opened_date
FROM products
WHERE ARRAY_CONTAINS(tags, 'premium')
ORDER BY opened_date;
