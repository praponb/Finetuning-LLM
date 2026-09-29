with src as (
    select *, round(credit_limit * 100, 2) as adjusted_credit_limit
    from customers
    where country = 'DE'
),
customers_stats as (
    select city, max(credit_limit) as max_credit_limit, max(signup_date) as last_signup_date
    from src
    group by city
)
select city, max_credit_limit, last_signup_date
from customers_stats
order by max_credit_limit desc
