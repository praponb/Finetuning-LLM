SELECT status FROM accounts WHERE MONTH(opened_date) IN (7, 9, 11) EXCEPT SELECT status FROM subscriptions WHERE monthly_fee <= 220
