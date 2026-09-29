SELECT txn_type, merchant, min_amount FROM (SELECT txn_type, merchant, MIN(amount) min_amount FROM transactions GROUP BY txn_type, merchant) x WHERE min_amount > 2600;
