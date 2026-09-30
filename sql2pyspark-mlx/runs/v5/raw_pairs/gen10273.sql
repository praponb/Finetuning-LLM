select id, balance from accounts where balance > (select avg(balance) from accounts) order by balance desc, 1 desc
