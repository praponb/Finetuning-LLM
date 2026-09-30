with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        inline(items)
    from carts
)
-- Window metrics over several different partitions
select
    customer_id,
    created_at,
    price,
    promo,
    avg(price) over w_running as price_avg_15m
from flattened
window w_running as (order by cast(created_at as bigint) asc range between 900 preceding and current row);
