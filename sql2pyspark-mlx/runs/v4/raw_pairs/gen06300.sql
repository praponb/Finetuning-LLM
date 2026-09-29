SELECT id, merchant, txn_type FROM transactions WHERE txn_type NOT IN ('FEE', 'REFUND') INTERSECT SELECT id, merchant, txn_type FROM transactions WHERE merchant IN ('Lazada', 'Shell', 'Starbucks');
