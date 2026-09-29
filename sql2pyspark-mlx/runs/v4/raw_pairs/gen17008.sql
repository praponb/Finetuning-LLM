SELECT merchant, account_id, txn_type, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END is_flagged_label, CONCAT(merchant, '/', txn_type) merchant_txn_type FROM transactions;
