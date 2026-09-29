select
    s.*,
    pr.id as promo_id,
    pr.promo_code as promotion_promo_code,
    pr.discount_pct as promotion_discount_pct
from promotions s
inner join promotions pr
    on s.channel = pr.channel
   and s.starts_on >= pr.starts_on
   and s.starts_on <= date_add(pr.ends_on, 7)
