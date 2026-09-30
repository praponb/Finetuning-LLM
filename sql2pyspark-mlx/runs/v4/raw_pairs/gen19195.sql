with filtered as (
    select
        id,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        cast(price as int) as price_int,
        coalesce(brand, 'NONE') as brand_filled
    from products
    where launch_date > to_date('2023-04-29')
      and supplier_id is not null
      and price <= 760
)
select
    date_trunc('quarter', launch_date) as quarter_start,
    supplier_id,
    round(avg(price), 2) as avg_price,
    approx_percentile(price_int, array(0.25, 0.5, 0.75)) as price_int_quartiles,
    approx_percentile(price, array(0.25, 0.5, 0.75)) as price_quartiles,
    grouping_id() as grouping_level
from filtered
group by grouping sets ((date_trunc('quarter', launch_date), supplier_id), (date_trunc('quarter', launch_date)))
order by supplier_id;
