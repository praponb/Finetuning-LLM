with valid as (
    -- Keep recent, valid rows
    select
        id,
        device,
        browser,
        started_at,
        to_date(started_at) started_at_date,
        lower(device) device_lower,
        coalesce(country, 'OTHER') resolved_country
    from sessions
    where started_at > date '2023-07-16'
      and country is not distinct from 'TH'
)
-- Window metrics over several different partitions
select
    *,
    row_number() over (partition by device order by started_at, id) seq_no,
    row_number() over (partition by browser order by started_at_date desc, id) seq_no_2,
    last_value(browser, true) over (partition by browser, device order by started_at_date, id rows between unbounded preceding and current row) last_known_browser
from valid
where started_at_date > '2024-08-01';
