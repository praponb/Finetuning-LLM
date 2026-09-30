select city, max(age) as max_age, round(stddev(age), 2) as stddev_age
from customers
where credit_limit >= 9400
group by city
order by stddev_age, max_age desc;
