with deduped as (
    -- Keep one row per key (latest first)
    select *
    from order_items
    qualify row_number() over (partition by product_id order by quantity desc, id asc) = 1
),
valid as (
    -- Keep recent, valid rows
    select *, unit_price + quantity line_total
    from deduped
    where product_id is not null
),
joined as (
    -- Attach orders attributes
    select
        e.*,
        p.order_date,
        p.discount,
        p.amount
    from valid e
    join orders p
        on e.order_id = p.id
),
metrics as (
    -- Ranking and gap features
    select
        *,
        avg(quantity) over (order by quantity asc, id asc rows between 3 preceding and 1 preceding) quantity_avg_3_prior,
        last_value(quantity, true) over (partition by product_id, order_id order by order_date asc, id asc rows between unbounded preceding and current row) last_known_quantity,
        lead(line_total, 1, 0) over (partition by product_id order by discount asc, id asc) next_line_total
    from joined
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        ntile(4) over (order by quantity_avg_3_prior asc, id asc) quantity_avg_3_prior_bucket,
        ntile(4) over (order by quantity desc, id asc) quantity_bucket,
        row_number() over (partition by order_id order by next_line_total desc, id asc) event_seq,
        row_number() over (partition by order_id order by quantity asc, id asc) row_seq
    from metrics
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        first_value(quantity_avg_3_prior, true) over (order by quantity_avg_3_prior_bucket asc, id asc rows between unbounded preceding and current row) first_known_quantity_avg_3_prior,
        first_value(quantity_avg_3_prior, true) over (partition by order_id order by unit_price asc, id asc rows between unbounded preceding and current row) first_known_quantity_avg_3_prior_2
    from with_windows
)
select
    id,
    order_id,
    product_id,
    line_total,
    discount,
    amount,
    last_known_quantity,
    next_line_total,
    quantity_avg_3_prior_bucket
from enriched
order by order_id desc;
