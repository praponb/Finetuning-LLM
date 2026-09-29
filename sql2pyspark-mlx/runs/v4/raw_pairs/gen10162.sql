with base as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        product_id,
        unit_price,
        unit_price + quantity as line_total
    from order_items
    where product_id is not null
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        order_id,
        line_total,
        unit_price
    from base
) src
unpivot (
    metric_value for kpi in (line_total, unit_price)
)
