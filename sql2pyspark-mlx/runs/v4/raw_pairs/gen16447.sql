with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode_outer(tier_limits) as val
    from price_tiers
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        plan,
        effective_from,
        currency,
        val,
        cume_dist() over (partition by plan, currency order by val nulls last) as val_cume_dist,
        dense_rank() over (order by val desc nulls first) as val_dense_rank
    from lines
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        count(val_cume_dist) over w_hist as plan_count_val_cume_dist,
        max(val_cume_dist) over w_hist as plan_max_val_cume_dist,
        rank() over w_grp as val_cume_dist_rank,
        avg(val_dense_rank) over w_part as currency_avg_val_dense_rank,
        cume_dist() over (partition by currency order by val_dense_rank desc) as val_dense_rank_cume_dist
    from scored
    window w_hist as (partition by plan), w_grp as (order by val_cume_dist), w_part as (partition by currency, plan)
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (order by val_dense_rank desc nulls first) as val_dense_rank_dense_rank,
        sum(plan_count_val_cume_dist) over (partition by currency) as currency_sum_plan_count_val_cume_dist
    from with_windows
),
with_windows_price_tiers as (
    -- Ranking and gap features
    select
        id,
        plan,
        effective_from,
        currency,
        val,
        val_cume_dist,
        val_dense_rank,
        plan_count_val_cume_dist,
        plan_max_val_cume_dist,
        val_cume_dist_rank,
        currency_avg_val_dense_rank,
        val_dense_rank_cume_dist,
        val_dense_rank_dense_rank,
        currency_sum_plan_count_val_cume_dist,
        rank() over (partition by plan, currency order by val_dense_rank_dense_rank desc nulls first) as val_dense_rank_dense_rank_rank,
        count(val) over (partition by plan) as plan_count_val,
        cume_dist() over w_hist as val_dense_rank_cume_dist_cume_dist
    from metrics
    window w_hist as (partition by currency, plan order by val_dense_rank_cume_dist desc nulls first)
),
enriched as (
    -- Ranking and gap features
    select
        *,
        min(val_dense_rank_dense_rank_rank) over w_hist as plan_min_val_dense_rank_dense_rank_rank,
        round(val_dense_rank_dense_rank / sum(val_dense_rank_dense_rank) over (partition by currency), 4) as val_dense_rank_dense_rank_share,
        count(val_dense_rank_cume_dist_cume_dist) over w_hist as plan_count_val_dense_rank_cume_dist_cume_dist,
        percent_rank() over (partition by currency order by val_dense_rank_cume_dist_cume_dist) as val_dense_rank_cume_dist_cume_dist_percent_rank
    from with_windows_price_tiers
    window w_hist as (partition by plan)
),
scored_price_tiers as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over w_hist as plan_count_val_dense_rank_cume_dist_cume_dist_percent_rank,
        dense_rank() over (partition by plan order by val_dense_rank_dense_rank_share desc nulls last) as val_dense_rank_dense_rank_share_dense_rank,
        dense_rank() over (partition by currency order by val_dense_rank_dense_rank desc) as val_dense_rank_dense_rank_dense_rank
    from enriched
    window w_hist as (order by plan_count_val_dense_rank_cume_dist_cume_dist desc)
)
-- Grouped metrics
select
    case when currency_sum_plan_count_val_cume_dist >= 84 then 'LARGE' else 'SMALL' end as currency_sum_plan_count_val_cume_dist_tier,
    case when val_dense_rank_dense_rank_share_dense_rank >= 8 then 'PREMIUM' else 'STANDARD' end as val_dense_rank_dense_rank_share_dense_rank_tier,
    sum(case when plan = 'enterprise' then plan_count_val_dense_rank_cume_dist_cume_dist_percent_rank else 0 end) as enterprise_plan_count_val_dense_rank_cume_dist_cume_dist_percent_rank,
    percentile_approx(plan_min_val_dense_rank_dense_rank_rank, array(0.25, 0.5, 0.75)) as plan_min_val_dense_rank_dense_rank_rank_quartiles,
    count(distinct plan) as distinct_plans,
    count(*) as event_count
from scored_price_tiers
group by 1, 2
order by event_count
