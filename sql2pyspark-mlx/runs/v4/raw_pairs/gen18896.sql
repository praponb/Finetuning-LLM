select *
from (
    select date_format(signup_date, 'yyyy-MM') as signup_month, round(avg(credit_limit), 2) as avg_credit_limit
    from customers
    where country in ('JP', 'UK')
    group by date_format(signup_date, 'yyyy-MM')
) as x
where avg_credit_limit > 71
order by signup_month desc
limit 50;
