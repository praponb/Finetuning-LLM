select round(stddev(sqft), 2) as stddev_sqft, round((100 * sum(case when city = 'Bangkok' then 1 else 0 end)) / count(*), 1) as pct_bangkok
from stores
order by pct_bangkok;
