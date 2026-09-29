with attributed as (
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from promotions e
    inner join promotions pr
        on e.channel <=> pr.channel
       and e.ends_on >= pr.starts_on
       and e.ends_on <= date_add(pr.ends_on, 3)
)
select
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    promo_id,
    promotion_promo_code,
    promotion_discount_pct
from attributed
order by discount_pct asc nulls first
