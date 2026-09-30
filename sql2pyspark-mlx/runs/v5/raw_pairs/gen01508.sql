with kept_keys as (
    -- Set difference on keys
    select promo_code
    from promotions
    except
    select promo_code
    from promotions
    where ends_on >= '2025-08-01'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from promotions s
    inner join kept_keys k on s.promo_code = k.promo_code
)
-- Grouped metrics
select
    promo_code,
    case when discount_pct >= 0.29 then 'HIGH' else 'LOW' end as discount_pct_tier,
    count_if(promo_code = 'SPRING10') as spring10_events,
    round(sum(discount_pct) / nullif(count(case when promo_code = 'BUNDLE' then 1 end), 0), 2) as discount_pct_per_bundle,
    count(*) as cnt
from kept_rows
group by promo_code, discount_pct_tier
having spring10_events > 1
order by discount_pct_per_bundle desc;
