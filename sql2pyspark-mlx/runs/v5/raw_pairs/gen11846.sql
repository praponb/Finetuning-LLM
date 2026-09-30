SELECT *
FROM transactions
WHERE (txn_type = 'FEE' AND is_flagged) OR txn_type IN ('DEBIT', 'REFUND', 'FEE')
ORDER BY amount DESC, merchant;
