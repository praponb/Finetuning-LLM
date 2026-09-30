with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, least(unit_price, quantity) as least_unit_price_quantity
    from order_items
    where product_id is not null
)
-- Join a dimension and aggregate
select
    dim.status,
    dim.channel,
    count(*) as num_rows,
    min(src.unit_price) as min_unit_price,
    grouping_id() as level_id
from cleaned src
inner join orders dim on src.order_id = dim.id
group by cube (dim.status, dim.channel)
order by status asc nulls first;
