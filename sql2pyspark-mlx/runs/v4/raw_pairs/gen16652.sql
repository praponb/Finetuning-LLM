SELECT
    accounts.id,
    accounts.opened_date,
    accounts.customer_id,
    accounts.status
FROM accounts
WHERE accounts.balance > 62000 AND accounts.customer_id IN (SELECT id FROM customers WHERE country = 'US');
