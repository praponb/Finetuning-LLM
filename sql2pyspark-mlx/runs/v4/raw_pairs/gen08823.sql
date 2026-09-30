with valid as (
    select
        id,
        order_id,
        unit_price,
        unit_price + quantity as line_total
    from order_items
    where product_id is not null
),
filtered as (
    select *, case when line_total >= 66 then 'High' when line_total >= 30 then 'Medium' else 'Low' end as line_total_level
    from valid
    where line_total > 66
),
with_windows as (
    select
        *,
        row_number() over (partition by order_id order by unit_price, id) as event_seq,
        percent_rank() over (partition by order_id order by line_total desc nulls first) as line_total_percent_rank,
        sum(line_total) over (order by line_total, id rows between unbounded preceding and current row) as running_line_total,
        sum(unit_price) over (partition by order_id order by unit_price, id rows between unbounded preceding and current row) as running_unit_price
    from filtered
)
select *
from (
    select
        id,
        order_id,
        unit_price,
        line_total
    from with_windows
) src
unpivot include nulls (
    val for metric in (unit_price, line_total as LINE_TOTAL_VAL)
)
