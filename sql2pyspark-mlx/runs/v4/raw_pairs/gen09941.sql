select amount, channel, cast(amount as int) as amount_int
from orders
order by channel;
