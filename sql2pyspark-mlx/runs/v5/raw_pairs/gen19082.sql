SELECT YEAR(txn_time) AS txn_year, MAX(txn_time) AS last_txn_time FROM transactions WHERE amount > 3000 AND account_id IN (2, 32, 35) AND merchant IN ('Shell', 'Grab', 'Starbucks') GROUP BY 1;
