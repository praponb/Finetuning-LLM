SELECT *, DATE_SUB(opened_date, 84) opened_date_minus_84d
FROM accounts
ORDER BY customer_id DESC, account_type
