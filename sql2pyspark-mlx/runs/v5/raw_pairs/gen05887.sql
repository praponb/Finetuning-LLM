with valid as (
    -- Keep recent, valid rows
    select *, least(units, revenue) least_units_revenue
    from store_sales
    where sale_date >= date '2023-09-02'
),
valid_store_sales as (
    -- Filter the raw rows and derive helper columns
    select *, units + revenue revenue_per_unit
    from valid
    where revenue > 1800
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from valid_store_sales
    qualify row_number() over (partition by product_id order by least_units_revenue desc nulls first, id asc) = 1
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        round(units / sum(units) over (partition by product_id, store_id), 4) units_share,
        sum(revenue_per_unit) over (order by units asc, id asc rows between unbounded preceding and current row) cumulative_revenue_per_unit
    from latest
    where store_id = 6
),
with_dims as (
    -- Attach products attributes
    select
        s.id,
        s.store_id,
        s.product_id,
        s.units,
        s.revenue,
        s.least_units_revenue,
        s.units_share,
        s.cumulative_revenue_per_unit,
        b.brand,
        b.launch_date
    from scored s
    left join products b
        on s.product_id = b.id
)
select
    id,
    store_id,
    product_id,
    units,
    least_units_revenue,
    units_share,
    cumulative_revenue_per_unit,
    brand,
    launch_date,
    case when brand rlike '^(Globex|Acme)$' then 'CORE' when brand rlike '^(Initech|Umbrella)$' then 'GROUP_A' else 'PRIORITY' end as brand_zone
from with_dims
order by product_id asc;
