select city
from customers
where signup_date >= '2023-04-01'
except
select city
from stores
where length(store_name) > 14;
