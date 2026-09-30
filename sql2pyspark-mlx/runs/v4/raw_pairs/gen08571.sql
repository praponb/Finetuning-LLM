select
    id,
    customer_id,
    started_at,
    browser,
    last_value(browser) over (partition by customer_id order by started_at, id rows between unbounded preceding and unbounded following) as last_browser
from sessions
order by id desc, browser
