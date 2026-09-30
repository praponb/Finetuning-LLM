with ranked_sessions as (
    select *, row_number() over (partition by country order by started_at desc, id) as rnk
    from sessions
)
select
    id,
    country,
    started_at,
    customer_id
from ranked_sessions
where rnk = 1
order by customer_id desc
