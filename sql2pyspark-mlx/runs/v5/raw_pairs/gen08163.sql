select
    id,
    country,
    age,
    ntile(4) over (partition by country order by age, id) as age_ntile
from customers
where credit_limit > 14700
