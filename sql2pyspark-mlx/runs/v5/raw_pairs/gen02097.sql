with base as (
    select
        *,
        unit_price - quantity as line_total,
        cast(unit_price as int) as unit_price_int
    from order_items
    where product_id is not null
),
enriched as (
    select
        s.id,
        s.order_id,
        s.product_id,
        s.quantity,
        s.unit_price,
        s.line_total,
        s.unit_price_int,
        d.shipping_fee,
        d.status
    from base s
    left join orders d
        on s.order_id = d.id
    where d.status = 'PENDING'
),
metrics as (
    select
        id,
        order_id,
        product_id,
        shipping_fee,
        avg(quantity) over (partition by product_id order by unit_price, id rows between 2 preceding and current row) as quantity_avg_2,
        rank() over (partition by status order by shipping_fee nulls first) as shipping_fee_rank
    from enriched
),
ranked as (
    select
        id,
        quantity_avg_2,
        shipping_fee_rank
    from metrics
    qualify row_number() over (partition by order_id order by shipping_fee desc nulls last, id) = 1
),
valid as (
    select
        *,
        shipping_fee_rank + quantity_avg_2 as shipping_fee_rank_plus_quantity_avg_2,
        coalesce(quantity_avg_2, 0) as quantity_avg_2_filled
    from ranked
    where shipping_fee_rank < 6
)
select
    case when shipping_fee_rank_plus_quantity_avg_2 >= 66 then 'HIGH' else 'LOW' end as shipping_fee_rank_plus_quantity_avg_2_tier,
    case when quantity_avg_2 >= 9 then 'LARGE' else 'SMALL' end as quantity_avg_2_tier,
    percentile_approx(quantity_avg_2_filled, 0.9) as quantity_avg_2_filled_p90,
    count(*) as cnt,
    round(avg(shipping_fee_rank), 2) as avg_shipping_fee_rank,
    max(shipping_fee_rank_plus_quantity_avg_2) as max_shipping_fee_rank_plus_quantity_avg_2
from valid
group by 1, 2
