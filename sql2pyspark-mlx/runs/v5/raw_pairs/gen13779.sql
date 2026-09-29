SELECT id, amount, is_flagged, account_id FROM transactions WHERE amount >= (SELECT MAX(amount) FROM transactions) ORDER BY id DESC, account_id;
