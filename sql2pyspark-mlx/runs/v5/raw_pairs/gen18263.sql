select
    date_trunc('MONTH', opened_date) as opened_month,
    round((100 * sum(case when account_type = 'CREDIT' then 1 else 0 end)) / count(*), 1) as pct_credit,
    count(case when status = 'CLOSED' then 1 end) as closed_count,
    sum(balance) as total_balance
from accounts
where account_type = 'LOAN'
group by 1
having count(case when status = 'CLOSED' then 1 end) >= 2;
