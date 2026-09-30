SELECT
    id,
    is_flagged,
    amount,
    account_id,
    CONCAT_WS('/', txn_type, merchant) AS txn_type_merchant_key
FROM transactions
ORDER BY account_id DESC;
