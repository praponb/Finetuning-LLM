with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        inline(items)
    from carts
),
joined as (
    -- Attach customers attributes
    select
        s.*,
        b.age,
        b.name
    from lines s
    inner join customers b
        on s.customer_id = b.id
),
base as (
    -- Basic cleansing and defaults
    select *, size(coupon_codes) as coupon_codes_count
    from joined
    where promo is distinct from 'VIP'
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        round(price / sum(price) over (partition by customer_id), 4) as price_share,
        round(age / sum(age) over (partition by promo, customer_id), 4) as age_share,
        dense_rank() over (order by product_id desc nulls last) as product_id_dense_rank
    from base
    where name is not null
)
select
    id,
    product_id,
    qty,
    promo,
    age,
    product_id_dense_rank,
    case when promo rlike '^(VIP|WELCOME5)$' then 'PRIORITY' when promo rlike '^(SPRING10|BUNDLE)$' then 'GROUP_B' else 'CORE' end as promo_zone
from metrics;
