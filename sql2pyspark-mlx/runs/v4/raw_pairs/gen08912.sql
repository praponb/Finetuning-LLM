with flattened as (
    select
        id,
        customer_id,
        pos,
        line.product_id,
        line.price,
        line.promo
    from carts
    lateral view posexplode(items) e as pos, line
)
select
    *,
    max(pos) over w_recent as customer_id_max_pos,
    min(product_id) over (partition by promo) as promo_min_product_id,
    row_number() over (partition by customer_id order by price, id, pos) as event_seq
from flattened
window w_recent as (partition by customer_id);
