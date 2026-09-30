select
    id,
    segment,
    city,
    signup_date,
    ntile(10) over (partition by segment, city order by signup_date desc, id) as quartile
from customers
order by id desc
