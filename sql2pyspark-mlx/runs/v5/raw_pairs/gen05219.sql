with ranked as (
    select *, row_number() over (partition by order_id, product_id order by unit_price nulls first, id) as latest_rank
    from order_items
    qualify latest_rank = 1
),
enriched as (
    select
        *,
        avg(unit_price) over (partition by product_id order by latest_rank, id rows between 3 preceding and 1 preceding) unit_price_avg_3_prior,
        first_value(unit_price) ignore nulls over (partition by order_id order by quantity, id rows between unbounded preceding and current row) first_known_unit_price,
        sum(quantity) over (order by latest_rank, id rows between unbounded preceding and current row) running_quantity
    from ranked
    where order_id in (9, 22, 30)
),
with_dims as (
    select
        a.*,
        p.stock_qty,
        p.product_name,
        p.category
    from enriched a
    left join products p
        on a.product_id = p.id
),
enriched_order_items as (
    select a.*, b.price
    from with_dims a
    left join products b
        on a.product_id = b.id
),
valid as (
    select
        *,
        concat_ws('/', category, product_name) category_product_name_key,
        upper(trim(product_name)) product_name_normalized
    from enriched_order_items
    where order_id in (12, 15)
)
select *
from (
    select
        id,
        product_id,
        first_known_unit_price,
        unit_price
    from valid
) src
unpivot (
    reading for metric_name in (first_known_unit_price, unit_price)
);
