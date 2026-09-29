select status, channel
from orders
where year(order_date) = 2025 or shipping_fee between 10 and 21
