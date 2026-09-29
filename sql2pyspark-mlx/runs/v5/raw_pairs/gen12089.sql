select account_id from transactions where amount > 1200 intersect select account_id from transactions where txn_type = 'DEBIT';
