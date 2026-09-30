with ranked as (
    select *
    from promotions
    qualify rank() over (partition by channel order by ends_on desc) = 1
)
select
    case when discount_pct >= 0.34 then 'LARGE' else 'SMALL' end as discount_pct_tier,
    min(discount_pct) as min_discount_pct,
    percentile_approx(discount_pct, array(0.25, 0.5, 0.75)) as discount_pct_quartiles,
    sum(case when promo_code = 'VIP' then discount_pct else 0 end) as vip_discount_pct,
    sort_array(collect_set(channel)) as channel_list,
    approx_percentile(discount_pct, array(0.25, 0.5, 0.75)) as discount_pct_quartiles_2
from ranked
group by case when discount_pct >= 0.34 then 'LARGE' else 'SMALL' end
having vip_discount_pct < 0
