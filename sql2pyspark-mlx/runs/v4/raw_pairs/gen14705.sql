SELECT a.id, a.balance, a.opened_date, a.account_type FROM accounts a WHERE a.id IN (SELECT account_id FROM transactions WHERE txn_type = 'REFUND');
