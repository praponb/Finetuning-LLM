select
    case when discount_pct >= 0.29 then 'LARGE' else 'SMALL' end as discount_pct_tier,
    approx_percentile(discount_pct, array(0.25, 0.5, 0.75)) as discount_pct_quartiles,
    count(case when channel = 'web' then 1 end) as web_count
from promotions
group by case when discount_pct >= 0.29 then 'LARGE' else 'SMALL' end
order by web_count nulls first
