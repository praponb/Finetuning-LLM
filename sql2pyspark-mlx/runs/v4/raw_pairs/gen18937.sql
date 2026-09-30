with kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from products s
    where exists (
            select 1
            from store_sales c
            where c.product_id = s.id
              and c.sale_date <= s.launch_date
        )
)
-- Multi-level aggregation
select
    date_trunc('quarter', launch_date) as quarter_start,
    brand,
    count(distinct category) as distinct_categories,
    round(avg(price), 2) as avg_price,
    count_if(brand = 'Stark') as stark_events,
    count(distinct supplier_id) as distinct_supplier_ids,
    round(sum(cost) / nullif(count(case when category = 'Electronics' then 1 end), 0), 2) as cost_per_electronics,
    sum(case when brand = 'Initech' then cost else 0 end) as initech_cost,
    grouping(date_trunc('quarter', launch_date)) as gid
from kept
group by grouping sets ((date_trunc('quarter', launch_date), brand), (date_trunc('quarter', launch_date)), ());
