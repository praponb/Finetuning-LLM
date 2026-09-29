with remaining_keys as (
    -- Set difference on keys
    select promo_code
    from promotions
    except distinct
    select promo_code
    from promotions
    where promo_code in ('VIP', 'SPRING10', 'BUNDLE')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from promotions s
    inner join remaining_keys k on s.promo_code = k.promo_code
)
-- Aggregate with conditional metrics
select
    case when discount_pct >= 0.33 then 'LARGE' else 'SMALL' end as discount_pct_tier,
    stddev_samp(discount_pct) as discount_pct_stddev,
    count_if(promo_code = 'WELCOME5') as welcome5_events,
    sum(case when channel = 'store' then discount_pct else 0 end) as store_discount_pct
from verified
group by rollup (case when discount_pct >= 0.33 then 'LARGE' else 'SMALL' end)
having store_discount_pct >= 0
order by discount_pct_tier desc;
