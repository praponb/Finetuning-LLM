select
    id,
    country,
    browser,
    customer_id,
    substring(browser, 1, 3) as browser_prefix,
    cast(started_at as date) as started_at_day
from sessions
where started_at >= '2023-07-01'
