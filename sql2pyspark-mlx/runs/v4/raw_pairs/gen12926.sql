with base as (
    select
        *,
        concat(device, ' - ', browser) as device_browser,
        lower(browser) as browser_lower
    from sessions
    where started_at >= date '2023-06-15'
)
select
    *,
    last_value(country, true) over (order by started_at, id rows between unbounded preceding and current row) as last_known_country,
    last_value(browser, true) over (partition by country order by started_at, id rows between unbounded preceding and current row) as last_known_browser
from base
