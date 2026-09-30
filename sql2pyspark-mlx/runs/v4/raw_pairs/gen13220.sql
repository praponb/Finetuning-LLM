with exploded as (
    select
        id,
        plan,
        effective_from,
        currency,
        element
    from price_tiers
    lateral view explode(discounts) x as element
),
with_windows as (
    select
        *,
        stddev_samp(element) over w_recent plan_stddev_samp_element,
        percent_rank() over (partition by plan, currency order by element desc nulls last) element_percent_rank,
        sum(element) over (partition by currency) currency_sum_element,
        max(element) over w_recent plan_max_element
    from exploded
    window w_recent as (partition by plan)
),
with_windows_price_tiers as (
    select
        *,
        cume_dist() over (partition by plan order by element nulls first) element_cume_dist,
        percent_rank() over (partition by currency order by currency_sum_element desc nulls last) currency_sum_element_percent_rank,
        cume_dist() over (order by element desc nulls last) element_cume_dist_2
    from with_windows
),
cleaned as (
    select *, element_cume_dist - currency_sum_element_percent_rank element_cume_dist_minus_currency_sum_element_percent_rank
    from with_windows_price_tiers
    where effective_from > date '2024-05-13'
),
scored as (
    select
        id,
        plan,
        effective_from,
        currency,
        element,
        plan_stddev_samp_element,
        element_percent_rank,
        currency_sum_element,
        plan_max_element,
        element_cume_dist,
        currency_sum_element_percent_rank,
        element_cume_dist_2,
        element_cume_dist_minus_currency_sum_element_percent_rank,
        rank() over (order by currency_sum_element_percent_rank) currency_sum_element_percent_rank_rank,
        stddev_samp(element) over (partition by currency, plan) currency_stddev_samp_element,
        min(currency_sum_element_percent_rank) over (partition by plan, currency) plan_min_currency_sum_element_percent_rank
    from cleaned
),
enriched as (
    select
        *,
        count(plan_max_element) over (partition by currency) currency_count_plan_max_element,
        dense_rank() over (order by plan_stddev_samp_element desc nulls last) plan_stddev_samp_element_dense_rank,
        cume_dist() over w_grp currency_sum_element_cume_dist
    from scored
    window w_grp as (order by currency_sum_element desc nulls first)
),
cleaned_price_tiers as (
    select
        plan,
        effective_from,
        currency,
        plan_stddev_samp_element,
        element_percent_rank,
        currency_sum_element,
        plan_max_element,
        element_cume_dist,
        currency_sum_element_percent_rank,
        element_cume_dist_2,
        element_cume_dist_minus_currency_sum_element_percent_rank,
        currency_sum_element_percent_rank_rank,
        currency_stddev_samp_element,
        plan_min_currency_sum_element_percent_rank,
        plan_stddev_samp_element_dense_rank,
        currency_sum_element_cume_dist,
        round(element_percent_rank, 1) element_percent_rank_rounded,
        round(currency_sum_element_cume_dist * 1.15, 2) adjusted_currency_sum_element_cume_dist
    from enriched
    where effective_from >= to_date('2024-05-29')
)
select
    date_trunc('year', effective_from) as year_start,
    sum(currency_sum_element_percent_rank_rank) filter (where plan = 'pro') as pro_total_currency_sum_element_percent_rank_rank,
    count(distinct currency) as distinct_currencies,
    approx_percentile(element_percent_rank_rounded, array(0.25, 0.5, 0.75)) as element_percent_rank_rounded_quartiles,
    grouping_id() as grouping_id
from cleaned_price_tiers
group by cube (date_trunc('year', effective_from))
having distinct_currencies >= 1
order by year_start desc nulls last;
