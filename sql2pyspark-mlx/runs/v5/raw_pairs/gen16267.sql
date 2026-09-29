with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        element
    from carts
    lateral view outer explode(coupon_codes) li as element
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        hour(created_at) as created_at_hour,
        case when element in ('SPRING10', 'WELCOME5') then 'Tier 1' else 'Other' end as element_group,
        coalesce(element, 'DIRECT') as element_clean
    from exploded
    where created_at >= to_date('2025-03-02')
      and created_at < '2024-05-01'
),
scored as (
    -- Window metrics over several different partitions
    select *, cume_dist() over (order by created_at_hour) as created_at_hour_cume_dist
    from filtered
),
filtered_carts as (
    -- Keep recent, valid rows
    select
        id,
        created_at,
        element,
        element_group,
        created_at_hour_cume_dist,
        created_at_hour / nullif(created_at_hour_cume_dist, 0) as created_at_hour_per_created_at_hour_cume_dist,
        coalesce(element, 'DIRECT') as resolved_element
    from scored
    where created_at > timestamp '2025-03-01 16:00:00'
      and element is not null
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        created_at,
        element,
        created_at_hour_cume_dist,
        created_at_hour_per_created_at_hour_cume_dist,
        cast(created_at as date) as created_at_day
    from filtered_carts
    where created_at >= timestamp '2025-03-01 11:00:00'
),
cleaned_carts as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        created_at,
        element,
        created_at_hour_per_created_at_hour_cume_dist,
        created_at_day,
        created_at_hour_per_created_at_hour_cume_dist - created_at_hour_cume_dist as created_at_hour_per_created_at_hour_cume_dist_minus_created_at_hour_cume_dist,
        coalesce(element, 'NONE') as element_filled
    from cleaned
    where created_at_hour_per_created_at_hour_cume_dist < 24
)
-- Grouped metrics
select
    date_trunc('day', created_at) as activity_day,
    case when created_at_hour_per_created_at_hour_cume_dist_minus_created_at_hour_cume_dist >= 81 then 'LARGE' else 'SMALL' end as created_at_hour_per_created_at_hour_cume_dist_minus_created_at_hour_cume_dist_tier,
    date_trunc('month', created_at_day) as activity_month,
    count(case when element = 'BUNDLE' then 1 end) as bundle_count,
    round(avg(created_at_hour_per_created_at_hour_cume_dist_minus_created_at_hour_cume_dist), 2) as avg_created_at_hour_per_created_at_hour_cume_dist_minus_created_at_hour_cume_dist,
    sort_array(collect_set(element)) as element_list,
    size(collect_set(element)) as num_elements,
    sum(case when element = 'SPRING10' then created_at_hour_per_created_at_hour_cume_dist_minus_created_at_hour_cume_dist else 0 end) as spring10_created_at_hour_per_created_at_hour_cume_dist_minus_created_at_hour_cume_dist
from cleaned_carts
group by date_trunc('day', created_at), case when created_at_hour_per_created_at_hour_cume_dist_minus_created_at_hour_cume_dist >= 81 then 'LARGE' else 'SMALL' end, date_trunc('month', created_at_day)
order by avg_created_at_hour_per_created_at_hour_cume_dist_minus_created_at_hour_cume_dist desc
