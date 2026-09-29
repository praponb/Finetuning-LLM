with enriched as (
    select
        id,
        starts_on,
        discount_pct,
        lead(discount_pct) over (partition by promo_code, channel order by ends_on, id) as next_discount_pct,
        lag(discount_pct) over (partition by promo_code, channel order by ends_on, id) as prev_discount_pct,
        row_number() over (partition by promo_code, channel order by ends_on, id) as event_seq,
        sum(discount_pct) over (partition by channel, promo_code) as channel_sum_discount_pct
    from promotions
    where starts_on < '2025-05-01'
)
select
    id,
    discount_pct,
    next_discount_pct,
    prev_discount_pct,
    event_seq,
    channel_sum_discount_pct
from enriched
order by discount_pct nulls first
