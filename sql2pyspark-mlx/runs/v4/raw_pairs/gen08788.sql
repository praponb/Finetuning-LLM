select
    date_trunc('week', ends_on) as week_start,
    size(collect_set(channel)) as num_channels,
    approx_percentile(discount_pct, array(0.25, 0.5, 0.75)) as discount_pct_quartiles,
    round(sum(discount_pct) / nullif(count(case when promo_code = 'VIP' then 1 end), 0), 2) as discount_pct_per_vip
from promotions
group by 1
having round(sum(discount_pct) / nullif(count(case when promo_code = 'VIP' then 1 end), 0), 2) > 0
