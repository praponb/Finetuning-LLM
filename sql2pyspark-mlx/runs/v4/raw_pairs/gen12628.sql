with flattened as (
    select
        id,
        customer_id,
        explode_outer(coupon_codes) as tag
    from carts
)
select
    customer_id,
    count(*) as event_count,
    count(*) filter (where tag = 'VIP') as vip_rows,
    count(*) filter (where tag = 'WELCOME5') as welcome5_rows,
    size(collect_set(tag)) as num_tags
from flattened
group by customer_id
order by welcome5_rows nulls first;
