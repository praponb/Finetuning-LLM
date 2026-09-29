select
    id,
    device,
    started_at,
    country,
    row_number() over w_hist as event_seq,
    first_value(country, true) over (partition by customer_id order by started_at asc, id asc rows between unbounded preceding and current row) as first_known_country
from sessions
where country in ('UK', 'US', 'SG')
window w_hist as (order by started_at asc, id asc)
