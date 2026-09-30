with valid as (
    -- Basic cleansing and defaults
    select
        *,
        revenue * units as revenue_per_unit,
        case when revenue >= 4400 then 'High' when revenue >= 800 then 'Medium' else 'Low' end as revenue_band
    from store_sales
    where sale_date > date '2023-04-04'
),
with_dims as (
    -- Attach products attributes
    select
        src.id,
        src.store_id,
        src.product_id,
        src.sale_date,
        src.units,
        src.revenue,
        src.revenue_band,
        d.brand
    from valid src
    inner join products d
        on src.product_id = d.id
    where d.supplier_id = 23
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        max(revenue) over w_ordered as running_max_revenue,
        sum(revenue) over w_ordered as running_sum_revenue,
        count(revenue) over w_ordered as running_count_revenue,
        sum(revenue) over w_hist as cumulative_revenue
    from with_dims
    window w_ordered as (partition by brand order by units, id rows between unbounded preceding and current row), w_hist as (partition by brand order by sale_date, id rows between unbounded preceding and current row)
),
ranked as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by brand order by running_sum_revenue desc nulls first) as dedup_rank
    from scored
    qualify dedup_rank = 1
),
cleaned as (
    -- Keep recent, valid rows
    select *, round(running_sum_revenue / running_count_revenue, 2) as running_sum_revenue_per_running_count_revenue
    from ranked
    where brand is distinct from 'Stark'
      and revenue < 3400
),
with_dims_store_sales as (
    -- Attach stores attributes
    select
        src.id,
        src.store_id,
        src.sale_date,
        src.units,
        src.revenue_band,
        src.brand,
        src.running_sum_revenue,
        src.running_count_revenue,
        src.cumulative_revenue,
        src.running_sum_revenue_per_running_count_revenue,
        b.region,
        b.city
    from cleaned src
    inner join stores b
        on src.store_id = b.id
)
-- Aggregate with conditional metrics
select
    city,
    brand,
    round(avg(cumulative_revenue), 2) as avg_cumulative_revenue,
    bit_or(units) as units_bit_or,
    stddev_samp(units) as units_stddev
from with_dims_store_sales
group by city, brand
