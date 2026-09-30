SELECT txn_type, merchant, CASE WHEN txn_type IN ('DEBIT', 'FEE') THEN 'Core' ELSE 'Other' END AS txn_type_group FROM transactions;
