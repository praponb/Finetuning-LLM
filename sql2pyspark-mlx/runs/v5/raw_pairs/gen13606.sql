select city, round(stddev(sqft), 2) as stddev_sqft, sum(sqft) as total_sqft
from stores
where month(opened_date) in (2, 3, 10) and sqft between 5861 and 9275
group by city;
