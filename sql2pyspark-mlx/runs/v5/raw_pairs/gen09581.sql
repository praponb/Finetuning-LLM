SELECT
    id,
    txn_type,
    ROUND(amount, 0) amount_rounded,
    CASE WHEN txn_type IN ('DEBIT', 'FEE') THEN 'Tier 1' ELSE 'Other' END txn_type_group
FROM transactions
WHERE txn_type IN ('FEE', 'DEBIT');
