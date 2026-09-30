SELECT merchant, YEAR(txn_time) AS txn_year, COUNT(*) AS num_transactions FROM transactions GROUP BY merchant, txn_year ORDER BY num_transactions, merchant, txn_year LIMIT 50;
