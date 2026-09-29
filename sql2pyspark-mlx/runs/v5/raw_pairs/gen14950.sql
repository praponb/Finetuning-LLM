select a.id, a.account_type, a.balance, a.opened_date from accounts a where a.balance = (select max(a2.balance) from accounts a2 where a2.account_type = a.account_type)
