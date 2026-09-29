with lines as (
    select
        id,
        plan,
        tier_value
    from price_tiers
    lateral view explode(tier_limits) t as tier_value
)
select
    id,
    plan,
    stddev_samp(tier_value) over (partition by plan) as plan_stddev_samp_tier_value,
    percent_rank() over w_recent as tier_value_percent_rank
from lines
where tier_value is null
window w_recent as (partition by plan order by tier_value desc nulls last);
