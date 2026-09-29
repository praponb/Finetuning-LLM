with flattened as (
    select
        id,
        plan,
        currency,
        val
    from price_tiers
    lateral view outer explode(tier_limits) li as val
),
with_windows as (
    select
        *,
        min(val) over w_ordered as plan_min_val,
        rank() over (partition by plan, currency order by val nulls last) as val_rank
    from flattened
    window w_ordered as (partition by plan)
),
enriched as (
    select *, percent_rank() over (order by val nulls first) as val_percent_rank
    from with_windows
),
with_windows_price_tiers as (
    select
        *,
        dense_rank() over (order by val desc nulls first) as val_dense_rank,
        sum(val) over w_recent as currency_sum_val
    from enriched
    window w_recent as (partition by currency)
),
metrics as (
    select *, round(val_dense_rank / sum(val_dense_rank) over (partition by currency), 4) as val_dense_rank_share
    from with_windows_price_tiers
),
with_windows_price_tiers_2 as (
    select
        plan,
        plan_min_val,
        val_dense_rank,
        currency_sum_val,
        round(val_dense_rank / sum(val_dense_rank) over (partition by currency), 4) as val_dense_rank_share_2,
        round(val_dense_rank / sum(val_dense_rank) over (partition by currency), 4) as val_dense_rank_share_3,
        percent_rank() over (partition by plan, currency order by val_rank desc nulls last) as val_rank_percent_rank,
        count(val_rank) over (partition by currency, plan) as currency_count_val_rank
    from metrics
),
scored as (
    select
        plan_min_val,
        currency_sum_val,
        val_dense_rank_share_2,
        val_dense_rank_share_3,
        currency_count_val_rank,
        cume_dist() over (partition by plan order by val_dense_rank_share_3 desc) as val_dense_rank_share_3_cume_dist,
        sum(val_rank_percent_rank) over (partition by plan) as plan_sum_val_rank_percent_rank,
        percent_rank() over (partition by plan order by plan_min_val desc) as plan_min_val_percent_rank,
        dense_rank() over (order by val_dense_rank_share_3 desc) as val_dense_rank_share_3_dense_rank
    from with_windows_price_tiers_2
)
select
    currency_sum_val,
    val_dense_rank_share_2,
    val_dense_rank_share_3,
    currency_count_val_rank,
    plan_sum_val_rank_percent_rank,
    plan_min_val_percent_rank,
    val_dense_rank_share_3_dense_rank
from scored
order by plan_min_val_percent_rank desc, plan_sum_val_rank_percent_rank;
