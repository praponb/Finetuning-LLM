select
    is_active,
    min(signup_date) as first_signup_date,
    max(credit_limit) - min(credit_limit) as credit_limit_range,
    max(credit_limit) as max_credit_limit
from customers
where age between 40 and 53 and name not like '%Grac%'
group by is_active
having max(credit_limit) < 6500
order by max_credit_limit desc
