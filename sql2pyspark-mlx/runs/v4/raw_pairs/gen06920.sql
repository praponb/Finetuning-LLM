select
    country,
    sum(credit_limit) as total_credit_limit,
    count(*) as cnt,
    rank() over (order by sum(credit_limit) desc) as credit_limit_rank
from customers
group by country
order by credit_limit_rank
