with cleaned as (
    select
        id,
        product_id,
        units,
        revenue,
        units - revenue revenue_per_unit,
        round(revenue, 1) revenue_rounded
    from store_sales
    where sale_date > to_date('2023-06-30')
)
select
    *,
    row_number() over (partition by product_id order by revenue, id) seq_no,
    sum(units) over (partition by product_id order by revenue_per_unit, id rows between unbounded preceding and current row) cumulative_units
from cleaned
where revenue between 1100 and 2000
