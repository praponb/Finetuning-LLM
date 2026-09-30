SELECT account_id, txn_time, DATE_TRUNC('YEAR', txn_time) AS txn_time_period FROM transactions WHERE txn_type IN ('REFUND', 'CREDIT', 'FEE') ORDER BY txn_time;
