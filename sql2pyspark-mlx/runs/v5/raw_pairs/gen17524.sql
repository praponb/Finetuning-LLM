SELECT id, txn_type, INITCAP(TRIM(txn_type)) AS txn_type_title FROM transactions WHERE (txn_type = 'REFUND' AND NOT is_flagged) OR txn_type = 'DEBIT'
