with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        element
    from price_tiers
    lateral view outer explode(tier_limits) t as element
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        min(element) over (partition by plan, currency) as plan_min_element,
        max(element) over (partition by currency) as currency_max_element
    from lines
),
metrics as (
    -- Ranking and gap features
    select
        id,
        plan,
        effective_from,
        currency,
        element,
        plan_min_element,
        currency_max_element,
        rank() over (partition by currency order by currency_max_element nulls first) as currency_max_element_rank,
        rank() over (order by element desc) as element_rank,
        dense_rank() over (partition by plan order by element nulls last) as element_dense_rank,
        cume_dist() over (partition by currency, plan order by plan_min_element nulls first) as plan_min_element_cume_dist
    from with_windows
),
base as (
    -- Basic cleansing and defaults
    select *, initcap(trim(currency)) as currency_title
    from metrics
    where currency_max_element_rank <= 7
),
metrics_price_tiers as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (partition by plan order by element_dense_rank nulls first) as element_dense_rank_percent_rank,
        round(currency_max_element_rank / sum(currency_max_element_rank) over (partition by currency), 4) as currency_max_element_rank_share,
        round(element_rank / sum(element_rank) over (partition by currency), 4) as element_rank_share
    from base
),
metrics_price_tiers_2 as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over w_ordered as element_dense_rank_cume_dist,
        count(element) over w_recent as currency_count_element
    from metrics_price_tiers
    window w_ordered as (partition by currency order by element_dense_rank nulls last), w_recent as (partition by currency)
),
by_type as (
    -- Native SQL PIVOT
    select *
    from (
        select
            currency,
            plan,
            element_dense_rank_percent_rank
        from metrics_price_tiers_2
    ) src
    pivot (
        sum(element_dense_rank_percent_rank) as value_sum,
        count(1) as n
        for plan in ('basic' as basic, 'enterprise' as enterprise, 'pro' as pro)
    )
)
select
    currency,
    basic_value_sum,
    basic_n,
    enterprise_value_sum,
    enterprise_n,
    pro_n,
    case when currency rlike '^(USD|THB)$' then 'CORE' when currency rlike '^(EUR)$' then 'OTHER_ZONE' else 'PRIORITY' end as currency_zone
from by_type;
