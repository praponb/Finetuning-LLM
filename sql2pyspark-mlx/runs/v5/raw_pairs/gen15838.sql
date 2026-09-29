select
    city,
    quarter(opened_date) as opened_quarter,
    max(sqft) as max_sqft,
    sum(sqft) as total_sqft,
    round((100 * sum(case when city = 'Tokyo' then 1 else 0 end)) / count(*), 1) as pct_tokyo
from stores
group by 1, 2
