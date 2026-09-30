select
    *,
    row_number() over (partition by customer_id, browser order by started_at, id) as seq_no,
    first_value(browser, true) over w_hist as first_known_browser,
    row_number() over (order by started_at desc, id) as row_seq
from sessions
window w_hist as (partition by customer_id order by started_at, id rows between unbounded preceding and current row)
