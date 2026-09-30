SELECT id, txn_type, account_id FROM transactions WHERE is_flagged = FALSE AND (merchant = 'Amazon' OR merchant = 'Starbucks') ORDER BY account_id;
