select month(signup_date) signup_month, round(stddev(age), 2) stddev_age
from customers
group by month(signup_date)
order by signup_month desc, 2 desc;
