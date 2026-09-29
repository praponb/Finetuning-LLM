SELECT
    account_type,
    MONTH(opened_date) opened_month,
    MIN(opened_date) min_opened_date,
    SUM(CASE WHEN account_type = 'CHECKING' THEN 1 ELSE 0 END) checking_count
FROM accounts
WHERE customer_id IS NOT NULL
GROUP BY account_type, MONTH(opened_date)
