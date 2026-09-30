with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        sale_date,
        units,
        revenue,
        date_format(sale_date, 'MMM yyyy') as sale_date_label
    from store_sales
    where store_id = 6
),
kept_keys as (
    -- Set difference on keys
    select id
    from cleaned
    except
    select id
    from store_sales
    where units = 14
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join kept_keys k on s.id = k.id
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        sale_date,
        revenue,
        sale_date_label,
        revenue + units as revenue_per_unit,
        case when revenue >= 4000 then 'High' when revenue >= 800 then 'Medium' else 'Low' end as revenue_tier
    from kept_rows
    where sale_date > date '2023-03-11'
      and revenue > 1400
),
metrics as (
    -- Ranking and gap features
    select *, percent_rank() over (order by revenue_per_unit desc) as revenue_per_unit_percent_rank
    from base
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        revenue,
        sale_date_label,
        revenue_per_unit,
        revenue_tier,
        cume_dist() over (order by revenue desc nulls last) as revenue_cume_dist,
        cume_dist() over (order by revenue_per_unit) as revenue_per_unit_cume_dist
    from metrics
),
grouped as (
    -- Aggregate with conditional metrics
    select
        case when revenue_per_unit_cume_dist >= 0.68 then 'HIGH' else 'LOW' end as revenue_per_unit_cume_dist_tier,
        percentile_approx(revenue_cume_dist, array(0.25, 0.5, 0.75)) as revenue_cume_dist_quartiles,
        count(distinct id) as distinct_ids
    from scored
    group by revenue_per_unit_cume_dist_tier
)
select
    revenue_per_unit_cume_dist_tier,
    revenue_cume_dist_quartiles,
    distinct_ids
from grouped
order by distinct_ids desc nulls last
