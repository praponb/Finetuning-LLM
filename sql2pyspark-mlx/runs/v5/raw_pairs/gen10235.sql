select
    id,
    email,
    country,
    city,
    is_active,
    ntile(5) over (partition by city order by credit_limit desc, id) as credit_limit_bucket,
    avg(age) over (partition by country, segment order by age, id rows between 5 preceding and 1 preceding) as age_avg_5_prior,
    corr(age, credit_limit) over (partition by city) as corr_age_credit_limit
from customers
