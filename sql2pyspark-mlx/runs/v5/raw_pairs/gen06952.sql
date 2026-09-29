select year(signup_date) as signup_year, max(age) as max_age
from customers
where not is_active and city <> 'Tokyo'
group by year(signup_date)
having count(age) < 1;
