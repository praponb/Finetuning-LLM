with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        v
    from carts
    lateral view explode(attributes) x as attr_key, v
),
flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        attr_key,
        v,
        explode(coupon_codes) as element
    from exploded
),
enriched as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.attr_key,
        e.v,
        e.element,
        d.credit_limit,
        d.email
    from flattened e
    left join customers d
        on e.customer_id = d.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        attr_key,
        v,
        element,
        email,
        avg(credit_limit) over (partition by customer_id) customer_id_avg_credit_limit,
        round(credit_limit / sum(credit_limit) over (partition by customer_id, attr_key), 4) credit_limit_share,
        percent_rank() over (order by credit_limit nulls first) credit_limit_percent_rank
    from enriched
)
-- Native SQL UNPIVOT (wide to long)
select
    customer_id,
    measure,
    val
from (
    select
        customer_id,
        credit_limit_share,
        credit_limit_percent_rank
    from with_windows
) src
unpivot (
    val for measure in (credit_limit_share as CREDIT_LIMIT_SHARE, credit_limit_percent_rank as CREDIT_LIMIT_PERCENT_RANK)
)
