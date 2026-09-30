with flattened as (
    select
        id,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    from carts
    lateral view outer explode(items) x as elem
),
lines as (
    select
        id,
        price,
        explode(coupon_codes) as tag
    from flattened
),
enriched as (
    select *, cume_dist() over (partition by tag order by price) as price_cume_dist
    from lines
),
enriched_carts as (
    select
        *,
        percent_rank() over w_part as price_percent_rank,
        count(price_cume_dist) over w_running as tag_count_price_cume_dist,
        rank() over (order by price desc nulls last) as price_rank,
        sum(price) over w_running as tag_sum_price,
        round(price / sum(price) over (partition by tag), 4) as price_share
    from enriched
    where price_cume_dist between 0.2 and 0.39
    window w_part as (partition by tag order by price desc nulls last), w_running as (partition by tag)
)
select
    id,
    price,
    tag,
    price_cume_dist,
    tag_count_price_cume_dist,
    price_share
from enriched_carts
order by id nulls last;
