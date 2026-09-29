SELECT id, amount, merchant FROM transactions WHERE account_id IN (12, 20, 26, 31) AND merchant IN ('Shell', 'Lazada', 'Amazon');
