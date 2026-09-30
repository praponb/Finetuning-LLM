with flattened as (
    select
        c.id,
        c.customer_id,
        c.created_at,
        pos + 1 as position,
        entry.product_id,
        entry.qty,
        entry.price,
        entry.promo
    from carts c
    lateral view posexplode(c.items) li as pos, entry
)
select
    product_id,
    max(created_at) as ended_at,
    min(created_at) as started_at
from flattened
group by product_id
order by product_id desc;
