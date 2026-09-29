select sum(shipping_fee) as total_shipping_fee, min(shipping_fee) as min_shipping_fee
from orders
order by total_shipping_fee;
