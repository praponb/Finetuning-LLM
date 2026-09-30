select
    *,
    avg(discount_pct) over (order by discount_pct, id rows between 5 preceding and 1 preceding) as discount_pct_avg_5_prior,
    avg(discount_pct) over (partition by channel order by starts_on, id rows between 5 preceding and current row) as discount_pct_avg_5
from promotions
