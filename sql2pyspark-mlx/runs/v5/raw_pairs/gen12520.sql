SELECT t.id, t.txn_time, t.is_flagged FROM transactions t WHERE t.is_flagged AND t.account_id IN (SELECT id FROM accounts WHERE opened_date <= '2024-07-01') ORDER BY t.txn_time ASC, t.id ASC
