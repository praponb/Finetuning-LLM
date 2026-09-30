select
    id,
    device,
    browser,
    started_at,
    upper(country) as country_upper,
    day(started_at) as started_at_day
from sessions
where customer_id = 6;
