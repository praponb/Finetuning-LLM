select
    id,
    device,
    started_at,
    country,
    first_value(country) over (partition by device order by started_at, id) as first_country
from sessions
where browser = 'Firefox'
