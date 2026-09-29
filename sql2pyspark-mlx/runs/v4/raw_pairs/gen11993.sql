SELECT
    c.segment,
    a.balance,
    a.account_type,
    a.opened_date
FROM customers c
LEFT JOIN accounts a ON c.id = a.customer_id;
