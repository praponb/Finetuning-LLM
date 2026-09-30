select
    id,
    signup_date,
    age,
    country,
    case when segment = 'Consumer' then 1 else 0 end as is_consumer
from customers
where (month(signup_date) in (1, 4, 6) and age between 41 and 43) or is_active = false
