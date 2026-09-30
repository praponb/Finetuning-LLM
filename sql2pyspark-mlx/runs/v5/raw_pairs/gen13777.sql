select case when balance >= 77000 then 'high' when balance >= 27000 then 'mid' else 'low' end as balance_bucket, min(balance) as min_balance, count(balance) as balance_count
from accounts
where year(opened_date) = 2024
group by balance_bucket
order by balance_bucket, balance_count
