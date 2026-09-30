select city, country
from customers
except
select city, country
from suppliers
order by country
