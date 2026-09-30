SELECT
    a.id,
    a.customer_id,
    a.opened_date,
    a.balance
FROM accounts a
WHERE a.status = 'FROZEN' AND a.customer_id NOT IN (SELECT id FROM customers WHERE email IS NOT NULL)
