select shipping_fee
from orders
union
select revenue
from store_sales
order by shipping_fee;
