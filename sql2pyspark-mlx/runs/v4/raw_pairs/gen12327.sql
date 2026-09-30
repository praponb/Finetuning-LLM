select
    date_trunc('quarter', started_at) as quarter_start,
    count_if(country = 'TH') as th_events,
    count(distinct country) as distinct_countries
from sessions
group by quarter_start
