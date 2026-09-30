SELECT id, amount, merchant, DAYOFWEEK(txn_time) AS txn_time_dayofweek, SUBSTRING(merchant, 1, 2) AS merchant_prefix FROM transactions ORDER BY merchant_prefix, merchant
