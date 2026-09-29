select sum(amount) as total_amount, count(case when merchant = '7-Eleven' then 1 end) as 7_eleven_count, avg(amount) as avg_amount from transactions order by total_amount desc, avg_amount asc;
