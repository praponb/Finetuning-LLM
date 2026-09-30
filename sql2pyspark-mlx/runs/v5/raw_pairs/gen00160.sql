with filtered_customers as (
    select *
    from customers
    where name like 'Ka%'
),
summary as (
    select case when country in ('TH', 'US') then 'group_a' else 'other' end as country_group, count(case when segment = 'Consumer' then 1 end) as consumer_count, max(age) as max_age
    from filtered_customers
    group by case when country in ('TH', 'US') then 'group_a' else 'other' end
)
select country_group, consumer_count, max_age
from summary
where max_age > 59
order by country_group desc;
