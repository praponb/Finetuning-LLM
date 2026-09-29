SELECT
    a.id,
    a.customer_id,
    a.opened_date,
    c.city
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.balance <= 61000 AND a.account_type IN ('CREDIT', 'SAVINGS', 'CHECKING');
