select
    case when discount_pct >= 0.29 then 'HIGH' else 'LOW' end as discount_pct_tier,
    channel,
    sort_array(collect_set(channel)) as channel_list,
    percentile_approx(discount_pct, 0.5) as discount_pct_p50
from promotions
group by grouping sets ((case when discount_pct >= 0.29 then 'HIGH' else 'LOW' end, channel), (case when discount_pct >= 0.29 then 'HIGH' else 'LOW' end))
order by channel desc
