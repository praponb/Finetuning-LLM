with flattened as (
    select
        id,
        created_at,
        coupon_codes,
        k,
        value
    from carts
    lateral view explode(attributes) e as k, value
)
select
    id,
    created_at,
    k,
    row_number() over w_ordered as seq_no,
    first_value(k) ignore nulls over (partition by k order by created_at, id, k rows between unbounded preceding and current row) as first_known_k,
    row_number() over w_ordered as row_seq
from flattened
window w_ordered as (partition by k order by created_at desc, id, k);
