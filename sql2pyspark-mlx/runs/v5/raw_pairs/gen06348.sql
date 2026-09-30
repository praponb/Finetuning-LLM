SELECT
    id,
    is_flagged,
    txn_time,
    account_id
FROM transactions
WHERE txn_time < '2024-01-01';
