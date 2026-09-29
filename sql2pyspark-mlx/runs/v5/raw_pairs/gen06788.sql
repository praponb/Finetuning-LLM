with base as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        quantity,
        unit_price,
        least(quantity, unit_price) as least_quantity_unit_price,
        cast(quantity as double) as quantity_num
    from order_items
    where product_id is not null
)
-- Window metrics over several different partitions
select
    id,
    quantity,
    unit_price,
    quantity_num,
    ntile(3) over (partition by order_id order by unit_price desc, id asc) as unit_price_bucket,
    row_number() over (partition by order_id order by quantity_num asc, id asc) as event_seq,
    lead(unit_price, 2, 0) over (partition by order_id order by unit_price asc, id asc) as next_unit_price,
    lead(unit_price, 1, 0) over (partition by order_id order by quantity_num asc, id asc) as next_unit_price_2,
    stddev_samp(quantity_num) over (partition by order_id) as order_id_stddev_samp_quantity_num
from base;
