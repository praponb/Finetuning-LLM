select customer_id from accounts where status = 'CLOSED' union all select customer_id from accounts where account_type in ('CHECKING', 'LOAN')
