select segment, city, max(signup_date) as last_signup_date
from customers
where signup_date >= '2024-08-25'
group by segment, city
order by last_signup_date desc;
