SELECT id, account_id, txn_time FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC, id) AS rnk FROM transactions) AS r WHERE rnk <= 1;
