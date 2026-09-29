SELECT
    a.id,
    a.opened_date,
    c.segment,
    c.country,
    c.name,
    DATEDIFF(CURRENT_DATE(), c.signup_date) AS days_since_signup_date
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
WHERE c.city IN ('Singapore', 'Berlin', 'Chiang Mai')
ORDER BY c.segment
