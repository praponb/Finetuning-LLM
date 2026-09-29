with exploded as (
    select
        id,
        effective_from,
        currency,
        element
    from price_tiers
    lateral view explode(tier_prices) li as element
),
metrics as (
    select
        *,
        percent_rank() over (order by element nulls first) as element_percent_rank,
        dense_rank() over (partition by currency order by element desc nulls last) as element_dense_rank
    from exploded
),
metrics_price_tiers as (
    select
        id,
        effective_from,
        currency,
        element,
        element_percent_rank,
        element_dense_rank,
        round(element_dense_rank / sum(element_dense_rank) over (partition by currency), 4) as element_dense_rank_share,
        stddev_samp(element) over (partition by currency) as currency_stddev_samp_element,
        avg(element_dense_rank) over (partition by currency) as currency_avg_element_dense_rank
    from metrics
)
select
    id,
    effective_from,
    element,
    element_dense_rank,
    currency_avg_element_dense_rank
from metrics_price_tiers
order by id desc nulls first;
