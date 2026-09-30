select
    id,
    country,
    started_at,
    rank() over (partition by country order by started_at) rnk
from sessions
where browser in ('Chrome', 'Safari')
order by rnk;
