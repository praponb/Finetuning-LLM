select
    id,
    customer_id,
    device,
    browser,
    started_at,
    country,
    row_number() over (partition by device, country order by started_at desc, id) as event_seq,
    row_number() over w_hist as event_seq_2,
    row_number() over w_recent as seq_no
from sessions
window w_hist as (partition by browser, customer_id order by started_at, id), w_recent as (partition by customer_id order by started_at, id)
