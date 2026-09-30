select id, balance, opened_date, account_type from accounts where balance < (select min(balance) from accounts);
