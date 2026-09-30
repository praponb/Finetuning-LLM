select *
from (
    select city, month(signup_date) as signup_month, sum(case when country = 'TH' then credit_limit else 0 end) as th_credit_limit
    from customers
    group by city, month(signup_date)
) as s
where th_credit_limit >= 36
order by signup_month desc, city asc;
