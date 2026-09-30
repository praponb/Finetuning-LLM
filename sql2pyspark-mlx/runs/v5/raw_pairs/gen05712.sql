select revenue
from store_sales
except
select amount
from orders;
