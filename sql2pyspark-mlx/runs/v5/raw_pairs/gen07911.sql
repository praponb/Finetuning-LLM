with flattened as (
    select
        id,
        plan,
        effective_from,
        tier_value
    from price_tiers
    lateral view outer explode(tier_limits) x as tier_value
),
with_windows as (
    select
        id,
        plan,
        tier_value,
        dense_rank() over (order by tier_value desc) as tier_value_dense_rank
    from flattened
    where plan = 'enterprise'
),
enriched as (
    select *, dense_rank() over (partition by plan order by tier_value) as tier_value_dense_rank_2
    from with_windows
),
kept_keys as (
    select id
    from enriched
    intersect all
    select id
    from price_tiers
    where currency in ('USD', 'EUR', 'THB')
),
kept_rows as (
    select s.*
    from enriched s
    inner join kept_keys k on s.id = k.id
),
with_windows_price_tiers as (
    select
        *,
        min(tier_value_dense_rank) over (partition by plan) as plan_min_tier_value_dense_rank,
        rank() over (order by tier_value desc nulls last) as tier_value_rank,
        rank() over (partition by plan order by tier_value desc) as tier_value_rank_2
    from kept_rows
),
cleaned as (
    select
        plan,
        tier_value_dense_rank_2,
        plan_min_tier_value_dense_rank,
        tier_value_rank,
        tier_value_rank_2,
        cast(tier_value as double) as tier_value_num
    from with_windows_price_tiers
    where plan = 'basic'
)
select
    case when plan_min_tier_value_dense_rank >= 9 then 'LARGE' else 'SMALL' end as plan_min_tier_value_dense_rank_tier,
    plan,
    case when tier_value_dense_rank_2 >= 7 then 'PREMIUM' else 'STANDARD' end as tier_value_dense_rank_2_tier,
    sort_array(collect_set(plan)) as plan_list,
    count(*) filter (where plan = 'starter') as starter_rows,
    percentile_approx(tier_value_rank_2, 0.9) as tier_value_rank_2_p90,
    sum(case when plan = 'enterprise' then plan_min_tier_value_dense_rank else 0 end) as enterprise_plan_min_tier_value_dense_rank,
    count(*) filter (where plan = 'starter') as starter_rows_2,
    round(sum(tier_value_dense_rank_2) / nullif(count(case when plan = 'enterprise' then 1 end), 0), 2) as tier_value_dense_rank_2_per_enterprise
from cleaned
group by plan_min_tier_value_dense_rank_tier, plan, tier_value_dense_rank_2_tier;
