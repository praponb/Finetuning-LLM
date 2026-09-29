with cleaned as (
    -- Basic cleansing and defaults
    select *, round(unit_price, 0) as unit_price_rounded
    from order_items
    where order_id = 27
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        count(quantity) over w_ordered as last3_count_quantity,
        min(quantity) over w_ordered as last3_min_quantity,
        avg(quantity) over w_ordered as last3_avg_quantity,
        sum(quantity) over w_ordered as last3_sum_quantity,
        dense_rank() over (partition by product_id, order_id order by unit_price_rounded desc) as unit_price_rounded_dense_rank
    from cleaned
    where quantity between 4 and 7
    window w_ordered as (partition by product_id, order_id order by unit_price, id rows between 2 preceding and current row)
),
base as (
    -- Keep recent, valid rows
    select *, round(quantity * 0.9, 2) as quantity_scaled
    from metrics
    where product_id is not null
      and unit_price_rounded_dense_rank >= 7
),
joined as (
    -- Attach orders attributes
    select
        a.*,
        d.status,
        d.amount
    from base a
    left join orders d
        on a.order_id = d.id
)
select
    s.id,
    s.order_id,
    s.product_id,
    s.quantity,
    s.unit_price,
    s.unit_price_rounded,
    s.last3_count_quantity,
    s.last3_min_quantity,
    s.last3_avg_quantity,
    (select avg(t2.unit_price) from order_items t2 where t2.order_id = s.order_id) as avg_order_item_unit_price
from joined s
