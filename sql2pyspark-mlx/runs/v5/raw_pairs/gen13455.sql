with base as (
    select *
    from customers
    where lower(trim(name)) = 'david lee' and lower(trim(email)) = 'henry@startup.io'
),
signup_quarter_summary as (
    select quarter(signup_date) as signup_quarter, round((100 * sum(case when segment = 'Corporate' then 1 else 0 end)) / count(*), 1) as pct_corporate
    from base
    group by quarter(signup_date)
)
select *
from signup_quarter_summary
where pct_corporate > 64
order by pct_corporate desc, signup_quarter
limit 50;
