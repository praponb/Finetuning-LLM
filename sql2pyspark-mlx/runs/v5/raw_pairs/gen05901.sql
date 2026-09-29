with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        explode_outer(tier_limits) as tier_value
    from price_tiers
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, case when currency in ('EUR', 'USD') then 'Tier 1' else 'Other' end currency_group
    from exploded
    where tier_value < 62
),
valid as (
    -- Basic cleansing and defaults
    select *, initcap(trim(plan)) plan_title
    from base
    where tier_value >= 28
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over (order by tier_value asc nulls first) tier_value_dense_rank,
        percent_rank() over (order by tier_value desc) tier_value_percent_rank,
        count(tier_value) over (partition by plan) plan_count_tier_value,
        percent_rank() over (partition by plan order by tier_value asc nulls last) tier_value_percent_rank_2,
        count(tier_value) over (partition by currency) currency_count_tier_value
    from valid
),
base_price_tiers as (
    -- Basic cleansing and defaults
    select *, case when tier_value >= 80 then 'High' when tier_value >= 22 then 'Medium' else 'Low' end tier_value_band
    from with_windows
    where currency_count_tier_value >= 8
)
-- Ranking and gap features
select
    *,
    max(tier_value) over w_ordered currency_max_tier_value,
    sum(tier_value_dense_rank) over (partition by currency, plan) currency_sum_tier_value_dense_rank,
    rank() over w_hist tier_value_percent_rank_2_rank,
    percent_rank() over (order by currency_count_tier_value asc nulls first) currency_count_tier_value_percent_rank
from base_price_tiers
where tier_value_percent_rank > 0.2
window w_ordered as (partition by currency), w_hist as (partition by plan order by tier_value_percent_rank_2 desc nulls last)
