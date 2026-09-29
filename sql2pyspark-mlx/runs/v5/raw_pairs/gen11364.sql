with valid_customers as (
    select *, case when segment in ('Consumer', 'Corporate') then 'Tier 1' else 'Other' end as segment_group
    from customers
    where email not like '%@gmail.com' or name not like 'E%'
),
customers_stats as (
    select month(signup_date) as signup_month, count(case when city = 'Tokyo' then 1 end) as tokyo_count
    from valid_customers
    group by month(signup_date)
)
select *
from customers_stats
order by signup_month desc
limit 10;
