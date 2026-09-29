select accounts.id, accounts.customer_id, accounts.opened_date from accounts where accounts.id in (select account_id from transactions where txn_type in ('FEE', 'REFUND', 'DEBIT'))
