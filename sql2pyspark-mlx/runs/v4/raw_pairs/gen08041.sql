with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        explode_outer(tier_limits) as element
    from price_tiers
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        plan,
        currency,
        element,
        sum(element) over (partition by currency) currency_sum_element,
        percent_rank() over w_recent element_percent_rank,
        dense_rank() over (order by element nulls last) element_dense_rank
    from lines
    window w_recent as (order by element)
),
enriched_price_tiers as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over w_part element_percent_rank_percent_rank,
        round(element_dense_rank / sum(element_dense_rank) over (partition by plan), 4) element_dense_rank_share,
        percent_rank() over (partition by currency order by element_dense_rank desc nulls first) element_dense_rank_percent_rank,
        avg(element) over w_running currency_avg_element,
        max(currency_sum_element) over w_running currency_max_currency_sum_element
    from enriched
    window w_part as (order by element_percent_rank desc nulls last), w_running as (partition by currency)
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        plan,
        currency,
        element,
        currency_sum_element,
        element_dense_rank,
        element_percent_rank_percent_rank,
        element_dense_rank_percent_rank,
        currency_avg_element,
        currency_max_currency_sum_element,
        element_dense_rank_percent_rank + currency_sum_element element_dense_rank_percent_rank_plus_currency_sum_element
    from enriched_price_tiers
    where currency_sum_element between 339 and 350
),
enriched_price_tiers_2 as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over w_ordered element_cume_dist,
        avg(element) over (partition by currency) currency_avg_element_2,
        stddev_samp(currency_max_currency_sum_element) over (partition by plan) plan_stddev_samp_currency_max_currency_sum_element
    from filtered
    window w_ordered as (partition by plan order by element nulls last)
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        plan,
        currency,
        element,
        currency_sum_element,
        element_dense_rank,
        element_percent_rank_percent_rank,
        element_dense_rank_percent_rank,
        currency_avg_element,
        currency_max_currency_sum_element,
        element_dense_rank_percent_rank_plus_currency_sum_element,
        element_cume_dist,
        currency_avg_element_2,
        plan_stddev_samp_currency_max_currency_sum_element,
        dense_rank() over (partition by currency order by currency_sum_element desc nulls last) currency_sum_element_dense_rank,
        dense_rank() over (partition by plan order by element_dense_rank desc) element_dense_rank_dense_rank,
        stddev_samp(element) over (partition by currency, plan) currency_stddev_samp_element,
        rank() over w_running element_rank
    from enriched_price_tiers_2
    window w_running as (partition by plan order by element desc)
)
select
    id,
    plan,
    currency,
    element,
    currency_sum_element,
    element_dense_rank,
    element_percent_rank_percent_rank,
    currency_avg_element,
    currency_max_currency_sum_element,
    case when plan rlike '^(basic|enterprise)$' then 'OTHER_ZONE' when plan rlike '^(starter|pro)$' then 'EXTENDED' else 'GROUP_A' end as plan_zone
from scored
order by element_dense_rank desc nulls last
