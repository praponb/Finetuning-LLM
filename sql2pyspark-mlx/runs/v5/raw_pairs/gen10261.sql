SELECT
    a.id,
    a.opened_date,
    a.account_type,
    c.city,
    INITCAP(TRIM(c.name)) AS name_title
FROM accounts a
JOIN customers c ON a.customer_id = c.id
WHERE c.city = 'Tokyo'
