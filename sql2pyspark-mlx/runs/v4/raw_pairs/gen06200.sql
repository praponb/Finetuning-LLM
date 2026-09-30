SELECT id, txn_type, TRIM(txn_type) clean_txn_type, INITCAP(TRIM(merchant)) merchant_title FROM transactions WHERE account_id IN (15, 30) ORDER BY txn_type ASC, merchant_title ASC;
