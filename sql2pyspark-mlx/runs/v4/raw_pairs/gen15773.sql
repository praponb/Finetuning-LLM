with kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from products s
    where not exists (
            select 1
            from order_items x
            where x.product_id = s.id
              and x.unit_price < 370
        )
)
-- Multi-level aggregation
select
    supplier_id,
    count(*) filter (where brand = 'Initech') as initech_rows,
    percentile_approx(stock_qty, 0.5) as stock_qty_p50,
    count(distinct category) as distinct_categories,
    count(*) as num_rows,
    size(collect_set(brand)) as num_brands,
    corr(stock_qty, cost) as corr_stock_qty_cost,
    grouping_id() as grouping_id
from kept
group by cube (supplier_id);
