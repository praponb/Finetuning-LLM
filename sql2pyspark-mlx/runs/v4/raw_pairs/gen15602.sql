select
    id,
    status,
    discount,
    channel,
    last_value(channel) over (partition by status order by discount, id rows between unbounded preceding and unbounded following) as last_channel
from orders
where channel is not null
