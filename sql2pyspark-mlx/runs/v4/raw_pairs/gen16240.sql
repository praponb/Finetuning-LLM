with enriched as (
    -- Attach orders attributes
    select a.*, d.amount
    from order_items a
    left join orders d
        on a.order_id = d.id
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by order_id order by amount desc, id) as latest_rank
    from enriched
    qualify latest_rank = 1
),
enriched_order_items as (
    -- Attach products attributes
    select
        e.*,
        d.stock_qty,
        d.cost,
        d.price
    from deduped e
    left join products d
        on e.product_id = d.id
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        avg(amount) over (partition by product_id order by amount, id rows between 2 preceding and current row) as amount_avg_2,
        ntile(3) over (order by amount desc, id) as amount_bucket
    from enriched_order_items
),
stats as (
    -- Aggregate with conditional metrics
    select
        order_id,
        case when amount_avg_2 >= 1209 then 'HIGH' else 'LOW' end as amount_avg_2_tier,
        product_id,
        corr(quantity, unit_price) as corr_quantity_unit_price,
        percentile_approx(cost, 0.5) as cost_p50,
        avg(stock_qty) as avg_stock_qty,
        round(avg(unit_price), 2) as avg_unit_price,
        bit_and(quantity) as quantity_bit_and,
        dense_rank() over (partition by order_id order by round(avg(unit_price), 2) desc) as avg_unit_price_dense_rank
    from with_windows
    group by order_id, amount_avg_2_tier, product_id
)
select
    order_id,
    amount_avg_2_tier,
    product_id,
    cost_p50,
    quantity_bit_and,
    avg_unit_price_dense_rank
from stats
order by order_id desc nulls first;
