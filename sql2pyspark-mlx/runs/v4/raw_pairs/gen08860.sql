select region, round(stddev(sqft), 2) as stddev_sqft
from stores
group by 1
order by region
limit 50;
