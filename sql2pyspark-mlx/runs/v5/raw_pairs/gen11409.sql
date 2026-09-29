with base as (
    -- Basic cleansing and defaults
    select
        id,
        product_id,
        quantity,
        unit_price,
        unit_price + quantity line_total
    from order_items
    where product_id is not null
),
ranked as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by product_id order by quantity desc nulls last) as rn
    from base
    qualify rn = 1
),
deduped as (
    -- QUALIFY-style window filter
    select
        id,
        product_id,
        rn
    from ranked
    qualify row_number() over (partition by product_id order by unit_price desc, id) = 1
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(rn, 0) rn_rounded,
        ceil(rn) rn_whole
    from deduped
    where product_id is null
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        sum(rn_whole) over (order by rn, id rows between unbounded preceding and current row) running_rn_whole,
        lead(rn_whole, 2) over (partition by product_id order by rn_whole, id) next_rn_whole
    from cleaned
),
enriched_order_items as (
    -- Attach products attributes
    select
        src.*,
        d.category,
        d.price
    from enriched src
    left join products d
        on src.product_id = d.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        lead(rn_whole) over (partition by category order by rn_whole, id) next_rn_whole_2,
        lag(running_rn_whole) over (partition by category order by rn_whole, id) prev_running_rn_whole,
        lead(running_rn_whole) over (partition by category order by rn_whole, id) next_running_rn_whole,
        row_number() over (partition by category order by rn_whole, id) seq_no
    from enriched_order_items
),
pivoted as (
    -- Native SQL PIVOT
    select *
    from (
        select
            product_id,
            category,
            next_running_rn_whole
        from with_windows
    ) src
    pivot (
        sum(next_running_rn_whole) as value_sum,
        count(1) as n
        for category in ('Books', 'Grocery')
    )
)
select
    product_id,
    Books_value_sum,
    Books_n,
    Grocery_value_sum,
    Grocery_n
from pivoted
order by Books_n desc nulls first;
