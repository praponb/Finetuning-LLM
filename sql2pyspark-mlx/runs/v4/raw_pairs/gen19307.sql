select city
from suppliers
union
select city
from stores
order by city desc;
