select date_format(signup_date, 'yyyy-MM') as signup_month, round(avg(credit_limit), 2) as avg_credit_limit
from customers
where email like '%@startup.io' and email like '%@company.co' and segment in ('Consumer', 'SMB', 'Corporate')
group by date_format(signup_date, 'yyyy-MM')
having round(avg(credit_limit), 2) > 43
order by avg_credit_limit desc
