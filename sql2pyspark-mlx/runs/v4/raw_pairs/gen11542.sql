with recent_customers as (
    select *
    from customers
    where segment != 'Consumer' and year(signup_date) >= 2023
),
signup_year_summary as (
    select year(signup_date) as signup_year, country, sum(age) as total_age
    from recent_customers
    group by year(signup_date), country
)
select *
from signup_year_summary
order by total_age desc, signup_year, country
limit 5
