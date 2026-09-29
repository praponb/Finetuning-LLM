with enriched as (
    -- Window metrics over several different partitions
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        min(discount_pct) over (partition by promo_code) as promo_code_min_discount_pct,
        avg(discount_pct) over (order by ends_on, id rows between 6 preceding and 1 preceding) as discount_pct_avg_6_prior
    from promotions
)
select
    id,
    promo_code,
    ends_on,
    discount_pct,
    discount_pct_avg_6_prior,
    case when promo_code rlike '^(WELCOME5|VIP)$' then 'GROUP_A' else 'PRIORITY' end as promo_code_zone
from enriched;
