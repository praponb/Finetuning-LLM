select
    id,
    channel,
    ends_on,
    last_value(channel, true) over (partition by channel order by starts_on, id rows between unbounded preceding and current row) as last_known_channel,
    row_number() over (order by ends_on desc, id) as row_seq,
    last_value(channel, true) over (partition by promo_code order by ends_on, id rows between unbounded preceding and current row) as last_known_channel_2
from promotions;
