select
    *,
    round(age / sum(age) over (partition by segment), 4) as age_share,
    last_value(segment, true) over (partition by country order by signup_date, id rows between unbounded preceding and current row) as last_known_segment,
    round(age / sum(age) over (partition by segment), 4) as age_share_2,
    sum(age) over (partition by country order by credit_limit, id rows between unbounded preceding and current row) as cumulative_age,
    last_value(segment, true) over (partition by segment order by age, id rows between unbounded preceding and current row) as last_known_segment_2
from customers
