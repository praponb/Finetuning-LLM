select status, account_type from accounts where customer_id is not null and account_type <> 'LOAN';
