select
    date_trunc('MONTH', opened_date) as opened_month,
    account_type,
    round(avg(balance), 2) as avg_balance,
    round((100 * sum(case when status = 'OPEN' then 1 else 0 end)) / count(*), 1) as pct_open
from accounts
group by date_trunc('MONTH', opened_date), account_type
order by avg_balance
