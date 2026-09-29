with valid_customers as (
    select *, case when country = 'UK' then 1 else 0 end as is_uk
    from customers
    where credit_limit <= 10700 or age < 40
),
city_summary as (
    select city, case when year(signup_date) < 2024 then 'before_2024' when year(signup_date) = 2024 then '2024' else 'after_2024' end as signup_cohort, round(stddev(is_uk), 2) as stddev_is_uk
    from valid_customers
    group by city, case when year(signup_date) < 2024 then 'before_2024' when year(signup_date) = 2024 then '2024' else 'after_2024' end
)
select *
from city_summary
where stddev_is_uk >= 52
order by city, signup_cohort
