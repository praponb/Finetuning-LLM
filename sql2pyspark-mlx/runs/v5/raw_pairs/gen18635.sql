with lines as (
    select
        id,
        plan,
        currency,
        val
    from price_tiers
    lateral view explode(discounts) x as val
),
cleaned as (
    select
        *,
        ceil(val) as val_whole,
        case when plan in ('basic', 'pro') then 'Core' else 'Other' end as plan_group
    from lines
    where plan != 'pro'
),
enriched as (
    select
        *,
        cume_dist() over (order by val nulls last) as val_cume_dist,
        dense_rank() over (partition by plan order by val desc nulls last) as val_dense_rank
    from cleaned
),
enriched_price_tiers as (
    select
        plan,
        currency,
        val,
        val_whole,
        plan_group,
        val_cume_dist,
        val_dense_rank,
        cume_dist() over (partition by currency order by val_whole desc nulls first) as val_whole_cume_dist,
        cume_dist() over (order by val nulls first) as val_cume_dist_2,
        dense_rank() over (partition by currency order by val_dense_rank nulls last) as val_dense_rank_dense_rank
    from enriched
)
select
    plan,
    val,
    val_whole,
    val_cume_dist,
    val_dense_rank,
    val_cume_dist_2,
    round(val_dense_rank / sum(val_dense_rank) over (partition by plan), 4) as val_dense_rank_share,
    percent_rank() over (partition by currency order by val_whole_cume_dist desc nulls first) as val_whole_cume_dist_percent_rank,
    cume_dist() over (partition by plan order by val_whole_cume_dist desc) as val_whole_cume_dist_cume_dist,
    min(val_whole_cume_dist) over (partition by currency, plan) as currency_min_val_whole_cume_dist,
    percent_rank() over (partition by plan order by val desc nulls first) as val_percent_rank
from enriched_price_tiers;
