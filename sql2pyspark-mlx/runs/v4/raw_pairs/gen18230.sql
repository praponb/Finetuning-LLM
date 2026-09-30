select
    id,
    country,
    city,
    signup_date,
    age,
    row_number() over (partition by country, city order by signup_date, id) as rn,
    first_value(signup_date) over (partition by country order by age desc, id) as first_signup_date
from customers
where segment is not null
order by signup_date;
