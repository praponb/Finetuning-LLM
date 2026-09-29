select txn_type, merchant from transactions where merchant in ('Lazada', 'Amazon') union select txn_type, merchant from transactions where txn_type = 'DEBIT' order by txn_type;
