select
    case when discount_pct >= 0.36 then 'LARGE' else 'SMALL' end as discount_pct_tier,
    promo_code,
    sum(case when channel = 'partner' then discount_pct else 0 end) as partner_discount_pct,
    count(*) as num_rows
from promotions
group by discount_pct_tier, promo_code
order by partner_discount_pct nulls first
