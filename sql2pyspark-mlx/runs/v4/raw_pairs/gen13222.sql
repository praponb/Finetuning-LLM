select
    id,
    customer_id,
    started_at,
    device
from (
    select
        id,
        customer_id,
        started_at,
        device,
        row_number() over (partition by customer_id order by started_at, id) as pos
    from sessions
) as r
where pos = 1
order by started_at desc, customer_id desc;
