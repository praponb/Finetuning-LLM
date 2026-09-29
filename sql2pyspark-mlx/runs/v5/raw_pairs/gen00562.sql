with cleaned as (
    select
        id,
        product_id,
        units,
        revenue,
        round(units, 0) as units_rounded
    from store_sales
    where sale_date > date '2023-06-18'
),
scored as (
    select
        id,
        product_id,
        revenue,
        units_rounded,
        sum(units_rounded) over (partition by product_id order by revenue, id rows between unbounded preceding and current row) as running_units_rounded,
        first_value(revenue) ignore nulls over (partition by product_id order by units, id rows between unbounded preceding and current row) as first_known_revenue,
        lead(revenue, 1) over (partition by product_id order by units_rounded, id) as next_revenue
    from cleaned
),
base as (
    select *, greatest(first_known_revenue, units_rounded) as greatest_first_known_revenue_units_rounded
    from scored
    where running_units_rounded < 110
),
remaining_keys as (
    select id, product_id
    from base
    intersect all
    select id, product_id
    from store_sales
    where units between 30 and 35
),
kept_rows as (
    select s.*
    from base s
    inner join remaining_keys k on s.id = k.id and s.product_id = k.product_id
),
valid as (
    select *, round(units_rounded * 1.07, 2) as adjusted_units_rounded
    from kept_rows
    where first_known_revenue > 2000
),
enriched as (
    select
        a.product_id,
        a.revenue,
        a.units_rounded,
        a.running_units_rounded,
        a.first_known_revenue,
        b.launch_date,
        b.cost,
        b.product_name
    from valid a
    left join products b
        on a.product_id = b.id
    where b.product_name is not null
)
select *
from (
    select
        product_id,
        cost,
        revenue,
        units_rounded
    from enriched
) src
unpivot (
    val for measure in (cost, revenue, units_rounded)
)
