select id, balance, customer_id from accounts where account_type in ('CREDIT', 'LOAN', 'SAVINGS') or customer_id in (10, 16) order by balance desc, id desc;
