with ranked as (
    select
        id,
        device,
        started_at,
        rank() over (partition by device order by started_at desc) rnk
    from sessions
    where customer_id is null
)
select *
from ranked
where rnk = 1
