select
    id,
    city,
    age,
    country,
    signup_date,
    avg(age) over (partition by city) as city_avg_age,
    lead(age, 1) over (partition by city, country order by signup_date, id) as next_age
from customers
where signup_date > '2024-06-01'
