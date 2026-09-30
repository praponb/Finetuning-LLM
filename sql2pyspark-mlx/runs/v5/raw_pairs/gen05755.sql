SELECT account_type FROM accounts EXCEPT SELECT account_type FROM accounts WHERE account_type IN ('CHECKING', 'LOAN', 'CREDIT')
