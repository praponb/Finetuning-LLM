SELECT status, account_type FROM accounts EXCEPT DISTINCT SELECT status, account_type FROM accounts WHERE account_type = 'CREDIT'
