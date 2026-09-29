SELECT
    id,
    txn_type,
    account_id,
    merchant
FROM transactions
WHERE (is_flagged AND is_flagged = FALSE) OR merchant IN ('Shell', '7-Eleven', 'Lazada');
