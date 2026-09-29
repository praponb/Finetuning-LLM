with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        units - revenue as revenue_per_unit
    from store_sales
    where sale_date > to_date('2023-07-25')
      and product_id in (3, 6)
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_per_unit,
        avg(revenue) over (partition by product_id) as product_id_avg_revenue,
        row_number() over (partition by store_id order by revenue_per_unit desc, id) as event_seq
    from cleaned
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        avg(units) over w_grp as units_avg_2,
        cume_dist() over (partition by store_id order by revenue) as revenue_cume_dist
    from scored
    window w_grp as (partition by store_id, product_id order by sale_date, id rows between 2 preceding and current row)
),
grouped as (
    -- Multi-level aggregation
    select
        case when revenue >= 3400 then 'LARGE' else 'SMALL' end as revenue_tier,
        store_id,
        approx_percentile(product_id_avg_revenue, array(0.25, 0.5, 0.75)) as product_id_avg_revenue_quartiles,
        count(*) as event_count,
        min(units_avg_2) as min_units_avg_2,
        max(event_seq) as max_event_seq,
        grouping(case when revenue >= 3400 then 'LARGE' else 'SMALL' end) as grouping_id
    from metrics
    group by cube (case when revenue >= 3400 then 'LARGE' else 'SMALL' end, store_id)
)
select
    s.revenue_tier,
    s.store_id,
    s.event_count,
    s.min_units_avg_2,
    s.max_event_seq,
    s.grouping_id,
    (select min(x.quantity) from inventory x where x.store_id = s.store_id) as min_inventory_quantity
from grouped s
order by grouping_id desc, min_units_avg_2
