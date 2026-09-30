select channel, count(distinct discount) as unique_channels
from orders
group by channel
having count(distinct discount) < 2
order by unique_channels desc;
