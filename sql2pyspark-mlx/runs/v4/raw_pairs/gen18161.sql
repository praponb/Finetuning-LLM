select
    id,
    customer_id,
    device,
    browser,
    started_at,
    country,
    row_number() over w_running as seq_no,
    row_number() over w_part as seq_no_2,
    row_number() over w_part as event_seq,
    first_value(browser) ignore nulls over w_grp as first_known_browser,
    last_value(browser, true) over w_grp as last_known_browser
from sessions
window w_running as (partition by country order by started_at, id), w_part as (partition by browser, customer_id order by started_at desc, id), w_grp as (partition by customer_id order by started_at, id rows between unbounded preceding and current row)
