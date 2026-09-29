select *, floor(balance) as balance_whole, cast(balance as int) as balance_int from accounts where balance >= 39000 order by opened_date desc;
