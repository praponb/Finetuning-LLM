with kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from customers s
    where exists (
            select 1
            from accounts x
            where x.customer_id = s.id
              and x.customer_id in (12, 28)
        )
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept s
    where not exists (
            select 1
            from accounts ch
            where ch.customer_id = s.id
              and ch.opened_date < s.signup_date
        )
),
base as (
    -- Keep recent, valid rows
    select *, trim(email) as clean_email
    from not_flagged
    where signup_date >= to_date('2023-03-11')
      and city is distinct from 'Tokyo'
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from base
    qualify row_number() over (partition by country order by signup_date desc, id) = 1
)
-- Multi-level aggregation
select
    city,
    date_trunc('year', signup_date) as year_start,
    round(avg(age), 2) as avg_age,
    count(*) as row_count,
    bit_or(age) as age_bit_or,
    percent_rank() over (partition by city order by count(*) desc) as row_count_percent_rank
from ranked
group by city, year_start
having row_count > 1
order by year_start desc;
