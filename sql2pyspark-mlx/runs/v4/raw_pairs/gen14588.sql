with base as (
    select *
    from stores
    where city in ('London', 'New York', 'Bangkok')
),
agg as (
    select
        region,
        year(opened_date) as opened_year,
        min(opened_date) as min_opened_date,
        round((100 * sum(case when region = 'West' then 1 else 0 end)) / count(*), 1) as pct_west
    from base
    group by region, year(opened_date)
)
select
    region,
    opened_year,
    min_opened_date,
    pct_west
from agg
where pct_west >= 71
order by region asc, 4 asc
