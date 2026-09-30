select city, pct_central
from (
    select city, round((100 * sum(case when region = 'Central' then 1 else 0 end)) / count(*), 1) as pct_central
    from stores
    where opened_date >= '2023-10-01'
    group by city
) as s
where pct_central < 20
order by city
