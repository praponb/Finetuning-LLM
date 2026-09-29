SELECT 'account' origin, opened_date event_date
FROM accounts
WHERE account_type IN ('SAVINGS', 'CHECKING')
UNION ALL
SELECT 'product' origin, launch_date event_date
FROM products
ORDER BY event_date;
