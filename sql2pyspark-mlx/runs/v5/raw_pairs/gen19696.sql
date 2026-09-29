SELECT t.id, t.amount, t.is_flagged, t.merchant FROM transactions t WHERE t.account_id NOT IN (SELECT id FROM accounts WHERE customer_id = 16);
