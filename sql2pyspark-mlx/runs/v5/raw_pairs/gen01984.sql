with not_flagged as (
    select s.*
    from orders s
    where not exists (
            select 1
            from shipments ch
            where ch.order_id = s.id
              and ch.delivered_date >= '2023-08-01'
        )
)
select
    id,
    store_id,
    order_date,
    status,
    channel,
    discount
from not_flagged
order by store_id;
