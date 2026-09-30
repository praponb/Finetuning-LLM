select city, round(stddev(age), 2) as stddev_age from customers where is_active and credit_limit between 11300 and 12200 and is_active group by city having round(stddev(age), 2) < 31;
