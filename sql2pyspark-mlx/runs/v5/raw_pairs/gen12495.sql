with with_windows as (
    select
        *,
        max(discount_pct) over (partition by promo_code order by discount_pct, id rows between unbounded preceding and current row) as running_max_discount_pct,
        count(discount_pct) over (partition by promo_code order by discount_pct, id rows between unbounded preceding and current row) as running_count_discount_pct,
        min(discount_pct) over (partition by promo_code order by discount_pct, id rows between unbounded preceding and current row) as running_min_discount_pct,
        last_value(channel) ignore nulls over (partition by promo_code order by discount_pct, id rows between unbounded preceding and current row) as last_known_channel
    from promotions
)
select
    id,
    channel,
    discount_pct,
    running_count_discount_pct,
    running_min_discount_pct,
    case when channel rlike '^(mobile|web)$' then 'GROUP_B' else 'CORE' end as channel_zone
from with_windows
order by id nulls last, channel_zone desc
