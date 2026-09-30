with valid as (
    -- Basic cleansing and defaults
    select *, unit_price - quantity line_total
    from order_items
    where unit_price >= 370
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        cast(quantity as double) quantity_str,
        line_total + unit_price line_total_plus_unit_price
    from valid
    where product_id is not null
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from filtered
    qualify row_number() over (partition by order_id, product_id order by line_total_plus_unit_price desc, id) = 1
),
enriched as (
    -- Attach products attributes
    select
        e.*,
        d.brand,
        d.product_name,
        d.price
    from deduped e
    join products d
        on e.product_id = d.id
),
enriched_order_items as (
    -- Ranking and gap features
    select
        *,
        lag(price, 2) over (partition by brand order by quantity, id) prev_price,
        lead(quantity_str, 1) over w_recent next_quantity_str,
        lag(line_total, 1) over w_part prev_line_total,
        avg(line_total) over (order by quantity_str, id rows between 3 preceding and 1 preceding) line_total_avg_3_prior
    from enriched
    window w_recent as (partition by product_id order by line_total, id), w_part as (partition by order_id, brand order by line_total, id)
),
enriched_order_items_2 as (
    -- Attach products attributes
    select a.*, p.category
    from enriched_order_items a
    left join products p
        on a.product_id = p.id
),
stats as (
    -- Aggregate with conditional metrics
    select
        order_id,
        count(*) as row_count,
        count(distinct order_id) as distinct_order_ids,
        bit_and(quantity) as quantity_bit_and,
        percentile_approx(next_quantity_str, array(0.25, 0.5, 0.75)) as next_quantity_str_quartiles
    from enriched_order_items_2
    group by cube (order_id)
)
select
    s.order_id,
    s.row_count,
    s.distinct_order_ids,
    s.quantity_bit_and,
    s.next_quantity_str_quartiles,
    (select sum(x.amount) from payments x where x.order_id = s.order_id) as sum_payment_amount
from stats s
order by sum_payment_amount;
