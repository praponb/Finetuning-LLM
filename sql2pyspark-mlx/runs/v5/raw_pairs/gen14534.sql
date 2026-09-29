SELECT t.id, t.amount, t.account_id, t.txn_time FROM transactions t WHERE t.account_id IN (19, 40) AND t.account_id NOT IN (SELECT id FROM accounts WHERE balance >= 46000);
