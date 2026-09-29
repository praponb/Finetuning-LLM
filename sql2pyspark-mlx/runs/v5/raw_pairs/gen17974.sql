select round((100 * sum(case when region = 'East' then 1 else 0 end)) / count(*), 1) as pct_east, max(sqft) as max_sqft
from stores
order by pct_east;
