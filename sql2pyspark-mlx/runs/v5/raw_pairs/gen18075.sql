with base as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        substring(device, 1, 2) as device_prefix
    from sessions
    where started_at > to_date('2023-06-22')
      and customer_id is not null
)
-- Window metrics over several different partitions
select
    id,
    device,
    country,
    first_value(browser, true) over (partition by country order by started_at, id rows between unbounded preceding and current row) as first_known_browser,
    last_value(country) ignore nulls over (partition by device order by started_at, id rows between unbounded preceding and current row) as last_known_country,
    last_value(country) ignore nulls over (partition by device order by started_at, id rows between unbounded preceding and current row) as last_known_country_2
from base;
