with enriched as (
    -- Running and moving aggregates
    select
        *,
        first_value(browser, true) over (order by started_at, id rows between unbounded preceding and current row) as first_known_browser,
        last_value(country, true) over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as last_known_country,
        row_number() over w_running as event_seq,
        row_number() over w_running_2 as seq_no
    from sessions
    window w_running as (order by started_at, id), w_running_2 as (partition by customer_id order by started_at desc, id)
)
select
    id,
    customer_id,
    device,
    first_known_browser,
    last_known_country,
    event_seq,
    seq_no,
    case when device rlike '^(mobile|tablet)$' then 'EXTENDED' when device rlike '^(desktop)$' then 'OTHER_ZONE' else 'GROUP_A' end as device_zone
from enriched
