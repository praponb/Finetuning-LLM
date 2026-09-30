select txn_type from transactions where amount <= 1700 union all select txn_type from transactions where account_id in (7, 25, 31);
