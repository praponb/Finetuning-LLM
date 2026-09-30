with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        explode(coupon_codes) as element
    from carts
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, hour(created_at) as created_at_hour
    from exploded
    where created_at > to_date('2025-03-02')
),
scored as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (partition by element order by created_at_hour nulls first) as created_at_hour_percent_rank,
        sum(created_at_hour) over w_recent as element_sum_created_at_hour,
        cume_dist() over (partition by element order by created_at_hour nulls last) as created_at_hour_cume_dist,
        avg(created_at_hour) over (partition by element order by cast(created_at as bigint) range between 7200 preceding and current row) as created_at_hour_avg_2h,
        stddev_samp(created_at_hour) over w_recent as element_stddev_samp_created_at_hour
    from valid
    window w_recent as (partition by element)
),
scored_carts as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (order by element_sum_created_at_hour desc nulls first) as element_sum_created_at_hour_percent_rank,
        sum(created_at_hour_percent_rank) over (partition by element) as element_sum_created_at_hour_percent_rank_2,
        stddev_samp(created_at_hour_percent_rank) over (partition by element) as element_stddev_samp_created_at_hour_percent_rank
    from scored
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        coalesce(element_sum_created_at_hour_percent_rank_2, 0) as element_sum_created_at_hour_percent_rank_2_filled,
        coalesce(element, 'NONE') as resolved_element
    from scored_carts
    where created_at > timestamp '2025-03-02 16:00:00'
      and element is not null
),
cleaned_carts as (
    -- Keep recent, valid rows
    select
        created_at,
        element,
        created_at_hour,
        created_at_hour_percent_rank,
        element_sum_created_at_hour,
        created_at_hour_cume_dist,
        created_at_hour_avg_2h,
        element_stddev_samp_created_at_hour,
        element_sum_created_at_hour_percent_rank,
        element_sum_created_at_hour_percent_rank_2,
        element_stddev_samp_created_at_hour_percent_rank,
        element_sum_created_at_hour_percent_rank_2_filled,
        resolved_element,
        least(created_at_hour_avg_2h, element_sum_created_at_hour_percent_rank) as least_created_at_hour_avg_2h_element_sum_created_at_hour_percent_rank,
        round(element_sum_created_at_hour_percent_rank, 1) as element_sum_created_at_hour_percent_rank_rounded
    from cleaned
    where created_at > date '2025-03-02'
      and element is not null
)
-- Grouped metrics
select
    case when element_sum_created_at_hour_percent_rank_2 >= 3 then 'PREMIUM' else 'STANDARD' end as element_sum_created_at_hour_percent_rank_2_tier,
    percentile_approx(element_sum_created_at_hour_percent_rank_rounded, 0.9) as element_sum_created_at_hour_percent_rank_rounded_p90,
    (unix_timestamp(max(created_at)) - unix_timestamp(min(created_at))) / 60 as span_minutes,
    grouping_id() as grouping_id
from cleaned_carts
group by rollup (case when element_sum_created_at_hour_percent_rank_2 >= 3 then 'PREMIUM' else 'STANDARD' end)
order by span_minutes nulls last
