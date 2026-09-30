with filtered as (
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        round(quantity * 0.85, 2) as quantity_scaled,
        quantity - unit_price as line_total
    from order_items
    where product_id is not null
),
valid as (
    select
        *,
        line_total * quantity as line_total_times_quantity,
        quantity_scaled + quantity as quantity_scaled_plus_quantity
    from filtered
    where line_total < 23
),
cleaned as (
    select
        id,
        quantity,
        unit_price,
        quantity_scaled,
        line_total,
        quantity_scaled_plus_quantity,
        coalesce(quantity, 0) as quantity_filled
    from valid
    where quantity_scaled < 66
),
cleaned_order_items as (
    select *, coalesce(line_total, 0) as line_total_filled
    from cleaned
    where quantity != 5
),
metrics as (
    select
        *,
        ntile(10) over w_running as line_total_bucket,
        avg(line_total_filled) over (order by quantity_scaled, id rows between 3 preceding and current row) as line_total_filled_avg_3,
        ntile(3) over w_running as line_total_bucket_2
    from cleaned_order_items
    window w_running as (order by line_total, id)
)
select
    case when quantity_filled >= 69 then 'PREMIUM' else 'STANDARD' end as quantity_filled_tier,
    approx_percentile(line_total, array(0.25, 0.5, 0.75)) as line_total_quartiles,
    round(avg(quantity_scaled_plus_quantity), 2) as avg_quantity_scaled_plus_quantity
from metrics
group by quantity_filled_tier;
