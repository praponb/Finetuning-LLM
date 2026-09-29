with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        element
    from price_tiers
    lateral view outer explode(tier_limits) li as element
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (order by element) as element_dense_rank,
        dense_rank() over (partition by plan, currency order by element nulls first) as element_dense_rank_2
    from flattened
),
with_windows_price_tiers as (
    -- Running and moving aggregates
    select
        id,
        plan,
        effective_from,
        currency,
        element,
        element_dense_rank,
        element_dense_rank_2,
        round(element_dense_rank / sum(element_dense_rank) over (partition by plan), 4) as element_dense_rank_share,
        min(element_dense_rank_2) over w_part as plan_min_element_dense_rank_2,
        min(element) over (partition by currency) as currency_min_element
    from with_windows
    window w_part as (partition by plan)
)
-- Multi-level aggregation
select
    plan,
    round(avg(currency_min_element), 2) as avg_currency_min_element,
    round(sum(element) / nullif(count(case when currency = 'EUR' then 1 end), 0), 2) as element_per_eur,
    count(distinct currency) as distinct_currencies,
    count(case when plan = 'basic' then 1 end) as basic_count,
    round(sum(element_dense_rank_2) / nullif(count(case when currency = 'THB' then 1 end), 0), 2) as element_dense_rank_2_per_thb
from with_windows_price_tiers
group by plan;
