select city, segment, min(signup_date) as min_signup_date
from customers
where credit_limit >= 8200
group by city, segment
order by min_signup_date desc, segment desc;
