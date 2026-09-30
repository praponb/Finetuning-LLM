SELECT opened_date, 'account' AS kind
FROM accounts
UNION ALL
SELECT launch_date, 'product' AS kind
FROM products
ORDER BY opened_date
