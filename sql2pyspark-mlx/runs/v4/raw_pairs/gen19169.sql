select
    id,
    browser,
    started_at,
    dense_rank() over (partition by browser order by started_at) as started_at_dense_rank
from sessions
where hour(started_at) between 12 and 18
order by browser desc
