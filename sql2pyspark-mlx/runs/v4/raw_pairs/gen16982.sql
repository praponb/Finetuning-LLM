with exploded as (
    select
        id,
        plan,
        effective_from,
        val
    from price_tiers
    lateral view explode(discounts) e as val
)
select
    *,
    dense_rank() over w_recent val_dense_rank,
    percent_rank() over (order by val) val_percent_rank,
    stddev_samp(val) over (partition by plan) plan_stddev_samp_val
from exploded
window w_recent as (partition by plan order by val desc nulls last);
