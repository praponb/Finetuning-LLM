SELECT
    id,
    is_flagged,
    txn_type,
    txn_time,
    CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
FROM transactions
