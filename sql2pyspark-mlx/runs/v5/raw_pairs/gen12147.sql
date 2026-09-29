select
    status,
    sum(balance) as total_balance,
    count(distinct balance) as unique_customers,
    count(case when account_type = 'CREDIT' then 1 end) as credit_count
from accounts
where customer_id = 35
group by status;
