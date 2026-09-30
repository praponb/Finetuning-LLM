with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        explode_outer(coupon_codes) as code
    from carts
)
-- Aggregate with conditional metrics
select
    customer_id,
    code,
    count(*) as cnt,
    size(collect_set(code)) as num_codes,
    count(distinct customer_id) as distinct_customer_ids,
    count(*) as cnt_2
from lines
group by customer_id, code
