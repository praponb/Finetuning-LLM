select city
from customers
where email like '%@outlook.com'
intersect
select city
from stores
order by city;
