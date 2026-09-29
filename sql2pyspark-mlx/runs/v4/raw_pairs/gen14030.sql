select * from (select case when year(signup_date) < 2024 then 'before_2024' when year(signup_date) = 2024 then '2024' else 'after_2024' end as signup_cohort, city, age from customers where signup_date >= '2024-08-20')
pivot (count(age) for city in ('Bangkok', 'London', 'New York', 'Berlin'))
order by signup_cohort;
