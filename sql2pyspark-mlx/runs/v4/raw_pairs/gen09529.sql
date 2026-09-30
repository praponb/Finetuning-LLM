select status, count(distinct balance) as unique_customers
from accounts
where status not in ('FROZEN', 'CLOSED') or account_type in ('CREDIT', 'SAVINGS', 'LOAN')
group by status
order by unique_customers desc, status
limit 50;
