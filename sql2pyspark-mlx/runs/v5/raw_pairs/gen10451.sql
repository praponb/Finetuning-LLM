select
    id,
    name,
    email,
    country,
    city,
    segment,
    signup_date,
    age
from customers
qualify row_number() over (partition by segment, city order by credit_limit, id) = 1
