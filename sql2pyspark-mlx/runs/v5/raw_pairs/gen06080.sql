select a.id, a.account_type, a.balance from accounts a where a.balance = (select min(b.balance) from accounts b where b.account_type = a.account_type) order by a.balance desc
