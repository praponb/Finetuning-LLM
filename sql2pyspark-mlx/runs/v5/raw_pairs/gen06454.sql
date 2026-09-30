with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, round(revenue * 100, 2) as adjusted_revenue
    from store_sales
    where sale_date between '2024-03-01' and '2025-01-17'
),
joined as (
    -- Attach products attributes
    select
        e.*,
        d.cost,
        d.category,
        d.price
    from cleaned e
    left join products d
        on e.product_id = d.id
    where d.product_name like 'La%'
),
ranked as (
    -- QUALIFY-style window filter
    select
        id,
        sale_date,
        units,
        revenue,
        cost,
        price
    from (
        select *, row_number() over (partition by store_id, product_id order by adjusted_revenue nulls first, id) as latest_rank
        from joined
    ) t
    where latest_rank = 1
),
remaining_keys as (
    -- Set difference on keys
    select id
    from ranked
    except distinct
    select id
    from store_sales
    where product_id in (9, 10)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from ranked s
    join remaining_keys k on s.id = k.id
),
filtered as (
    -- Basic cleansing and defaults
    select *, cost + price as margin
    from kept_rows
    where sale_date > date '2023-06-11'
      and price between 370 and 850
),
with_windows as (
    -- Window metrics over several different partitions
    select *, rank() over (order by revenue desc) as revenue_rank
    from filtered
),
with_windows_store_sales as (
    -- Window metrics over several different partitions
    select *, cume_dist() over (order by price desc nulls first) as price_cume_dist
    from with_windows
)
-- Grouped metrics
select
    date_trunc('year', sale_date) as year_start,
    round(avg(revenue), 2) as avg_revenue,
    count(distinct id) as distinct_ids,
    count(*) as event_count,
    bit_or(units) as units_bit_or
from with_windows_store_sales
group by date_trunc('year', sale_date)
order by units_bit_or desc nulls first;
