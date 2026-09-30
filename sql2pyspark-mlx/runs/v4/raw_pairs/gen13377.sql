with kept_keys as (
    select channel, promo_code
    from promotions
    intersect
    select channel, promo_code
    from promotions
    where promo_code in ('WELCOME5', 'BUNDLE', 'VIP')
),
reconciled as (
    select s.*
    from promotions s
    inner join kept_keys k on s.channel = k.channel and s.promo_code = k.promo_code
)
select
    date_trunc('week', ends_on) as week_start,
    case when discount_pct >= 0.34 then 'HIGH' else 'LOW' end as discount_pct_tier,
    channel,
    sort_array(collect_set(channel)) as channel_list,
    count(case when promo_code = 'WELCOME5' then 1 end) as welcome5_count,
    count(distinct promo_code) as distinct_promo_codes,
    stddev_samp(discount_pct) as discount_pct_stddev,
    grouping(date_trunc('week', ends_on)) as gid
from reconciled
group by rollup (date_trunc('week', ends_on), case when discount_pct >= 0.34 then 'HIGH' else 'LOW' end, channel);
