with flattened as (
    select
        id,
        plan,
        effective_from,
        currency,
        explode_outer(tier_limits) as element
    from price_tiers
)
select
    date_trunc('week', effective_from) as week_start,
    count(case when currency = 'THB' then 1 end) as thb_count,
    count(*) as num_rows,
    count(*) as event_count
from flattened
group by rollup (date_trunc('week', effective_from))
having count(*) < 1000
order by event_count desc
