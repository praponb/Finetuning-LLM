with lines as (
    select
        id,
        plan,
        currency,
        explode_outer(discounts) as val
    from price_tiers
),
with_windows as (
    select
        *,
        avg(val) over w_recent as plan_avg_val,
        dense_rank() over (partition by currency, plan order by val desc) as val_dense_rank,
        count(val) over (partition by currency, plan) as currency_count_val
    from lines
    window w_recent as (partition by plan)
),
enriched as (
    select
        *,
        cume_dist() over w_part as val_dense_rank_cume_dist,
        min(currency_count_val) over (partition by currency) as currency_min_currency_count_val,
        rank() over (order by plan_avg_val) as plan_avg_val_rank,
        sum(currency_count_val) over (partition by plan) as plan_sum_currency_count_val,
        round(val_dense_rank / sum(val_dense_rank) over (partition by currency), 4) as val_dense_rank_share
    from with_windows
    window w_part as (order by val_dense_rank desc)
)
select
    id,
    plan,
    currency,
    val,
    val_dense_rank,
    currency_min_currency_count_val,
    plan_avg_val_rank,
    plan_sum_currency_count_val,
    val_dense_rank_share,
    case when plan rlike '^(enterprise|basic)$' then 'PRIORITY' else 'EXTENDED' end as plan_zone
from enriched;
