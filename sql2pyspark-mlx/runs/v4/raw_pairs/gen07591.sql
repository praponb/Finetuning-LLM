select
    id,
    name,
    email,
    is_active,
    age + credit_limit age_plus_credit_limit,
    greatest(age, credit_limit) greatest_age_credit_limit
from customers
order by 2
