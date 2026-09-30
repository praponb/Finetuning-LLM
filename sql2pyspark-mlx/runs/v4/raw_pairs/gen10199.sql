select city, avg(sqft) as avg_sqft, round(stddev(sqft), 2) as stddev_sqft
from stores
group by city
having stddev_sqft >= 63
