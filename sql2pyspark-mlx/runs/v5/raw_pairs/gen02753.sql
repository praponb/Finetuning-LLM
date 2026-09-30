select count(distinct credit_limit) as unique_countrys, avg(age) as avg_age
from customers
order by 2, unique_countrys desc;
