SELECT status FROM orders WHERE channel IN ('web', 'store', 'partner') UNION ALL SELECT status FROM accounts WHERE account_type IN ('CHECKING', 'LOAN');
