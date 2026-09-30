select country
from customers
where email is not null
union
select country
from customers
where city = 'New York'
order by country;
