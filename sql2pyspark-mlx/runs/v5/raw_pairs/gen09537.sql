with lines as (
    select
        id,
        effective_from,
        explode_outer(tier_prices) as element
    from price_tiers
),
with_windows as (
    select
        id,
        element,
        percent_rank() over (order by element desc nulls last) as element_percent_rank
    from lines
),
base as (
    select *, ceil(element_percent_rank) as element_percent_rank_whole
    from with_windows
    where element < 40
),
remaining_keys as (
    select id
    from base
    except
    select id
    from price_tiers
    where plan = 'pro'
),
kept_rows as (
    select s.*
    from base s
    inner join remaining_keys k on s.id = k.id
),
base_price_tiers as (
    select *, element_percent_rank + element_percent_rank_whole as element_percent_rank_plus_element_percent_rank_whole
    from kept_rows
    where element_percent_rank <= 0.61
)
select
    case when element >= 67 then 'HIGH' else 'LOW' end as element_tier,
    case when element_percent_rank >= 0.76 then 'PREMIUM' else 'STANDARD' end as element_percent_rank_tier,
    case when element_percent_rank_plus_element_percent_rank_whole >= 87 then 'LARGE' else 'SMALL' end as element_percent_rank_plus_element_percent_rank_whole_tier,
    count(distinct id) as distinct_ids,
    count(distinct id) as distinct_ids_2
from base_price_tiers
group by case when element >= 67 then 'HIGH' else 'LOW' end, case when element_percent_rank >= 0.76 then 'PREMIUM' else 'STANDARD' end, case when element_percent_rank_plus_element_percent_rank_whole >= 87 then 'LARGE' else 'SMALL' end
