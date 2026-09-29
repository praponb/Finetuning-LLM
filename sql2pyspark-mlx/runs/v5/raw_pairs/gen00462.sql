with lines as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        explode_outer(discounts) as val
    from price_tiers
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, last_day(effective_from) as effective_from_month_end
    from lines
    where currency = 'USD'
),
cleaned as (
    -- Basic cleansing and defaults
    select
        effective_from,
        currency,
        val,
        case when currency in ('THB', 'USD') then 'Group A' else 'Other' end as currency_group,
        datediff(current_date(), effective_from_month_end) as days_since_effective_from_month_end
    from valid
    where year(effective_from) >= 2025
),
valid_price_tiers as (
    -- Basic cleansing and defaults
    select *, quarter(effective_from) as effective_from_quarter
    from cleaned
    where val <= 79
),
enriched as (
    -- Running and moving aggregates
    select
        val,
        days_since_effective_from_month_end,
        cume_dist() over (partition by currency order by effective_from_quarter nulls first) as effective_from_quarter_cume_dist,
        cume_dist() over (partition by currency order by days_since_effective_from_month_end nulls last) as days_since_effective_from_month_end_cume_dist,
        count(effective_from_quarter) over (partition by currency) as currency_count_effective_from_quarter
    from valid_price_tiers
    where days_since_effective_from_month_end <> 50
)
select
    val,
    days_since_effective_from_month_end,
    days_since_effective_from_month_end_cume_dist,
    currency_count_effective_from_quarter
from enriched
order by currency_count_effective_from_quarter desc nulls last
