SELECT
    DATE_FORMAT(c.signup_date, 'yyyy-MM') signup_month,
    c.segment,
    SUM(CASE WHEN a.account_type = 'LOAN' THEN 1 ELSE 0 END) loan_count,
    MAX(a.opened_date) latest_opened_date
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.status = 'CLOSED'
GROUP BY DATE_FORMAT(c.signup_date, 'yyyy-MM'), c.segment;
