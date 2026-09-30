with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        quantity,
        unit_price,
        quantity * unit_price as line_total,
        case when unit_price >= 440 then 'High' when unit_price >= 130 then 'Medium' else 'Low' end as unit_price_band
    from order_items
    where product_id is not null
      and order_id in (6, 28)
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        order_id,
        unit_price,
        line_total
    from filtered
) src
unpivot include nulls (
    val for metric_name in (unit_price, line_total as LINE_TOTAL)
);
