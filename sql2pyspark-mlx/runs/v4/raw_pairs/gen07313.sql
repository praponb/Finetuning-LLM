with filtered as (
    -- Keep recent, valid rows
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        lower(promo_code) as promo_code_lower,
        coalesce(channel, 'NONE') as channel_filled
    from promotions
    where channel is not null
)
-- Running and moving aggregates
select
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    promo_code_lower,
    channel_filled,
    lead(discount_pct, 1, 0) over (partition by promo_code order by ends_on, id) as next_discount_pct,
    avg(discount_pct) over (partition by promo_code, channel order by starts_on, id rows between 2 preceding and 1 preceding) as discount_pct_avg_2_prior,
    sum(discount_pct) over (partition by channel, promo_code order by discount_pct, id rows between unbounded preceding and current row) as cumulative_discount_pct
from filtered
