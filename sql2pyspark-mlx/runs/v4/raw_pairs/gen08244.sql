SELECT status FROM accounts WHERE account_type NOT IN ('LOAN', 'CREDIT', 'SAVINGS') UNION SELECT status FROM subscriptions;
