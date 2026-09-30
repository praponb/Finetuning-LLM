SELECT
    t.id,
    t.txn_type,
    t.merchant,
    t.is_flagged
FROM transactions t
WHERE t.merchant IN ('Starbucks', 'Grab', 'Lazada') AND t.account_id NOT IN (SELECT id FROM accounts WHERE status IN ('FROZEN', 'CLOSED', 'OPEN'))
ORDER BY merchant;
