with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, ceil(quantity) as quantity_whole
    from inventory
    where store_id in (3, 4)
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, quantity_whole - quantity as quantity_whole_minus_quantity
    from cleaned
    where last_restock > date '2023-04-07'
),
base_inventory as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        greatest(quantity_whole_minus_quantity, quantity) as greatest_quantity_whole_minus_quantity_quantity,
        quantity_whole * quantity as quantity_whole_times_quantity
    from base
    where last_restock > to_date('2023-07-04')
),
enriched as (
    -- Attach products attributes
    select
        a.*,
        d.price,
        d.brand
    from base_inventory a
    inner join products d
        on a.product_id = d.id
),
enriched_inventory as (
    -- Attach products attributes
    select
        e.id,
        e.product_id,
        e.last_restock,
        e.greatest_quantity_whole_minus_quantity_quantity,
        e.quantity_whole_times_quantity,
        p.launch_date,
        p.cost
    from enriched e
    left join products p
        on e.product_id = p.id
    where p.launch_date < '2025-09-01'
)
-- Aggregate with conditional metrics
select
    case when cost >= 550 then 'HIGH' else 'LOW' end as cost_tier,
    date_trunc('year', launch_date) as year_start,
    date_trunc('month', last_restock) as activity_month,
    avg(cost) as avg_cost,
    count(*) as cnt
from enriched_inventory
group by case when cost >= 550 then 'HIGH' else 'LOW' end, date_trunc('year', launch_date), date_trunc('month', last_restock)
order by year_start nulls last
