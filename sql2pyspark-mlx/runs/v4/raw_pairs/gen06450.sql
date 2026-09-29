with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        round(quantity / unit_price, 2) as line_total
    from order_items
    where product_id = 14
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by order_id order by unit_price desc nulls last, id) as dedup_rank
    from filtered
    qualify dedup_rank = 1
),
with_dims as (
    -- Attach orders attributes
    select
        src.*,
        o.amount,
        o.order_date
    from deduped src
    join orders o
        on src.order_id = o.id
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        ntile(5) over w_part as unit_price_bucket,
        sum(unit_price) over w_recent as product_id_sum_unit_price
    from with_dims
    window w_part as (partition by order_id order by unit_price, id), w_recent as (partition by product_id, order_id)
)
-- Running and moving aggregates
select
    *,
    ntile(10) over w_recent as quantity_bucket,
    ntile(4) over (partition by product_id order by product_id_sum_unit_price desc, id) as product_id_sum_unit_price_bucket,
    row_number() over w_recent as row_seq,
    ntile(4) over (partition by order_id order by amount, id) as amount_bucket
from with_windows
window w_recent as (partition by product_id order by quantity desc, id);
