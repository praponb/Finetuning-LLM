select status, total_balance from (select status, sum(balance) as total_balance from accounts group by status) as x where total_balance < 261000 order by status desc, total_balance desc limit 5
