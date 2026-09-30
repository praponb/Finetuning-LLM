with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        currency,
        val
    from price_tiers
    lateral view outer explode(tier_limits) x as val
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, case when currency in ('USD', 'EUR') then 'Core' else 'Other' end as currency_group
    from exploded
    where val is not null
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        currency,
        rank() over (order by val nulls last) as val_rank,
        percent_rank() over (partition by currency order by val nulls last) as val_percent_rank
    from valid
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        count(val_percent_rank) over (partition by currency) as currency_count_val_percent_rank,
        rank() over (partition by currency order by val_percent_rank desc nulls last) as val_percent_rank_rank,
        max(val_rank) over (partition by currency) as currency_max_val_rank
    from enriched
)
-- Multi-level aggregation
select
    currency,
    sum(val_rank) as total_val_rank,
    sum(case when currency = 'USD' then val_percent_rank_rank else 0 end) as usd_val_percent_rank_rank,
    count(*) as row_count,
    count_if(currency = 'USD') as usd_events,
    count(*) as row_count_2
from metrics
group by currency
having count_if(currency = 'USD') < 1000
