SELECT account_id, is_flagged, txn_type, IF(is_flagged, 'Yes', 'No') AS is_flagged_label FROM transactions
