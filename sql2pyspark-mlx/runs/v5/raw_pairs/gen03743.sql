select
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    max(discount_pct) over (partition by channel order by ends_on, id rows between 3 preceding and current row) as last4_max_discount_pct,
    sum(discount_pct) over (partition by channel order by ends_on, id rows between 3 preceding and current row) as last4_sum_discount_pct,
    min(discount_pct) over (partition by channel order by ends_on, id rows between 3 preceding and current row) as last4_min_discount_pct,
    avg(discount_pct) over (partition by channel order by ends_on, id rows between 3 preceding and current row) as last4_avg_discount_pct,
    rank() over (partition by promo_code order by discount_pct) as discount_pct_rank
from promotions
