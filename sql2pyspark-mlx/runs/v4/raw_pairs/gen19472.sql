with Ranked as (
    select
        id,
        customer_id,
        started_at,
        rank() over (partition by customer_id order by started_at desc) as rnk
    from sessions
)
select id, customer_id, started_at
from Ranked
where rnk = 1;
