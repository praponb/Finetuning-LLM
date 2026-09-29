SELECT
    a.id,
    a.customer_id,
    a.account_type,
    a.opened_date
FROM accounts a
WHERE a.opened_date BETWEEN '2023-11-01' AND '2024-11-01' AND a.id IN (SELECT account_id FROM transactions WHERE amount < 2200);
