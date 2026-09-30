with src as (
    select *
    from customers
    where month(signup_date) in (1, 2, 9)
),
Summary as (
    select
        country,
        segment,
        count(case when segment = 'Corporate' then 1 end) as corporate_count,
        round(avg(age), 2) as avg_age
    from src
    group by country, segment
)
select country, corporate_count, avg_age
from Summary
