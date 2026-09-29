select quarter(signup_date) as signup_quarter, min(signup_date) as min_signup_date
from customers
group by quarter(signup_date)
order by signup_quarter desc
limit 5;
