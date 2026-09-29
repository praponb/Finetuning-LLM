with valid as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        units + revenue as revenue_per_unit,
        cast(revenue as int) as revenue_int
    from store_sales
    where store_id = 4
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        revenue,
        revenue_per_unit,
        ntile(4) over w_part as units_bucket,
        first_value(revenue_per_unit) ignore nulls over w_running as first_known_revenue_per_unit,
        dense_rank() over (partition by store_id order by units) as units_dense_rank,
        corr(units, revenue) over (partition by product_id) as corr_units_revenue
    from valid
    window w_part as (partition by product_id, store_id order by units, id), w_running as (partition by store_id, product_id order by revenue_int, id rows between unbounded preceding and current row)
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        last_value(units_bucket) ignore nulls over (partition by store_id order by corr_units_revenue, id rows between unbounded preceding and current row) as last_known_units_bucket,
        first_value(revenue, true) over (partition by store_id order by first_known_revenue_per_unit, id rows between unbounded preceding and current row) as first_known_revenue
    from scored
),
matched_keys as (
    -- Set intersection on keys
    select store_id
    from enriched
    intersect all
    select store_id
    from store_sales
    where units <> 30
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join matched_keys k on s.store_id = k.store_id
),
filtered as (
    -- Keep recent, valid rows
    select *, round(revenue_per_unit, 2) as revenue_per_unit_rounded
    from kept_rows
    where corr_units_revenue is not null
)
select
    s.id,
    s.store_id,
    s.revenue,
    s.revenue_per_unit,
    s.units_dense_rank,
    s.corr_units_revenue,
    s.last_known_units_bucket,
    s.first_known_revenue,
    s.revenue_per_unit_rounded,
    (select max(o2.shipping_fee) from orders o2 where o2.store_id = s.store_id) as max_order_shipping_fee
from filtered s
order by revenue_per_unit_rounded desc, corr_units_revenue
