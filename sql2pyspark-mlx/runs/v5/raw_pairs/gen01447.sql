select
    id,
    customer_id,
    country,
    row_number() over (partition by device, country order by started_at, id) as seq_no,
    row_number() over (partition by device order by started_at, id) as row_seq,
    row_number() over (partition by customer_id, browser order by started_at desc, id) as event_seq,
    first_value(browser) ignore nulls over w_grp as first_known_browser,
    last_value(browser) ignore nulls over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as last_known_browser
from sessions
window w_grp as (partition by browser order by started_at, id rows between unbounded preceding and current row);
