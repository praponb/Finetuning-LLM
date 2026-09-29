with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        k,
        v
    from carts
    lateral view outer explode(attributes) t as k, v
)
-- Aggregate with conditional metrics
select
    k,
    count(*) as num_rows,
    size(collect_set(k)) as num_ks
from flattened
group by k
