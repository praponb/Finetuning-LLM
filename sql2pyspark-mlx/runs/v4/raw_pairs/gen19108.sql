with valid as (
    -- Keep recent, valid rows
    select *, round(quantity * 1.15, 2) as quantity_adjusted
    from order_items
    where product_id is not null
      and order_id in (10, 19)
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_id,
        quantity,
        unit_price,
        quantity_adjusted,
        unit_price / nullif(quantity_adjusted, 0) as unit_price_per_quantity_adjusted
    from valid
    where product_id in (27, 35, 37)
),
enriched as (
    -- Ranking and gap features
    select
        id,
        product_id,
        quantity,
        unit_price,
        quantity_adjusted,
        unit_price_per_quantity_adjusted,
        row_number() over (order by quantity desc, id) as row_seq,
        first_value(unit_price) ignore nulls over (partition by product_id order by quantity_adjusted, id rows between unbounded preceding and current row) as first_known_unit_price
    from cleaned
),
metrics as (
    -- Ranking and gap features
    select
        *,
        lag(row_seq, 1) over (partition by product_id order by first_known_unit_price, id) as prev_row_seq,
        avg(unit_price) over (partition by product_id order by quantity, id rows between 5 preceding and current row) as unit_price_avg_5,
        avg(quantity) over (order by row_seq, id rows between 6 preceding and 1 preceding) as quantity_avg_6_prior,
        ntile(10) over (partition by product_id order by unit_price_per_quantity_adjusted, id) as unit_price_per_quantity_adjusted_bucket
    from enriched
)
select
    id,
    product_id,
    unit_price,
    quantity_adjusted,
    unit_price_per_quantity_adjusted,
    row_seq,
    prev_row_seq,
    unit_price_avg_5,
    quantity_avg_6_prior
from metrics
order by prev_row_seq desc nulls last
