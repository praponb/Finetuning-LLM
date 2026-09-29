select
    id,
    is_active,
    credit_limit,
    country,
    concat(email, ' ', city) as email_city
from customers
where year(signup_date) = 2024;
