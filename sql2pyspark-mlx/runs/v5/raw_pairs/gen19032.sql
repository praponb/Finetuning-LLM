with valid as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        case when revenue >= 3300 then 'High' when revenue >= 1100 then 'Medium' else 'Low' end revenue_band
    from store_sales
    where sale_date > date '2023-06-14'
      and product_id in (1, 5, 8, 9)
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        revenue,
        revenue_band,
        lag(units) over (partition by store_id order by sale_date, id) prev_units,
        lead(units) over (partition by store_id order by sale_date, id) next_units,
        lag(revenue) over (partition by store_id order by sale_date, id) prev_revenue,
        row_number() over (partition by store_id order by sale_date, id) row_seq
    from valid
),
metrics as (
    -- Ranking and gap features
    select
        *,
        lag(next_units, 1, 0) over (partition by store_id order by revenue, id) prev_next_units,
        ntile(4) over (order by revenue, id) revenue_bucket
    from enriched
),
valid_store_sales as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        revenue,
        next_units,
        row_seq,
        prev_next_units,
        revenue_bucket,
        round(next_units / prev_next_units, 2) next_units_per_prev_next_units
    from metrics
    where revenue_bucket < 7
)
select
    s.id,
    s.store_id,
    s.next_units,
    s.revenue_bucket,
    s.next_units_per_prev_next_units,
    (select max(x.units) from store_sales x where x.store_id = s.store_id) as max_store_sale_units
from valid_store_sales s
order by store_id desc nulls first;
