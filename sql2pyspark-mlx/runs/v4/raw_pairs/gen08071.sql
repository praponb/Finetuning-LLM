with filtered as (
    select
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        weekofyear(ends_on) as ends_on_weekofyear,
        coalesce(channel, 'N/A') as resolved_channel
    from promotions
    where channel is not null
)
select
    case when discount_pct >= 0.36 then 'LARGE' else 'SMALL' end as discount_pct_tier,
    promo_code,
    date_trunc('quarter', starts_on) as quarter_start,
    round(sum(discount_pct) / nullif(count(case when promo_code = 'WELCOME5' then 1 end), 0), 2) as discount_pct_per_welcome5,
    count_if(promo_code = 'SPRING10') as spring10_events,
    percentile_approx(discount_pct, array(0.25, 0.5, 0.75)) as discount_pct_quartiles,
    count(*) as cnt,
    round(avg(ends_on_weekofyear), 2) as avg_ends_on_weekofyear,
    sum(case when channel = 'web' then ends_on_weekofyear else 0 end) as web_ends_on_weekofyear
from filtered
group by 1, 2, 3
order by avg_ends_on_weekofyear desc;
