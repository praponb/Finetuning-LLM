select country, round(avg(credit_limit), 2) as avg_credit_limit
from customers
group by country
having avg_credit_limit >= 43
order by 2, country
limit 5;
