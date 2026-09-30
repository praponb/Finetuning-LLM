SELECT id, account_id, txn_type, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label, CAST(amount AS INT) AS amount_int FROM transactions WHERE is_flagged AND txn_type = 'CREDIT'
