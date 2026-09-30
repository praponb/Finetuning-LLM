with base as (
    select
        id,
        order_id,
        quantity,
        round(quantity / unit_price, 2) as line_total
    from order_items
    where product_id is not null
      and quantity is null
),
deduped as (
    select
        id,
        order_id,
        quantity,
        line_total
    from base
    qualify row_number() over (partition by order_id order by line_total desc nulls last, id) = 1
),
base_order_items as (
    select *, coalesce(line_total, 0) as line_total_filled
    from deduped
    where order_id in (2, 15, 18, 32)
),
base_order_items_2 as (
    select
        id,
        order_id,
        quantity,
        line_total_filled,
        cast(line_total as int) as line_total_int,
        ceil(quantity) as quantity_whole
    from base_order_items
    where quantity < 3
)
select
    d.status,
    count(*) as rows_total,
    round(sum(s.line_total_int), 2) as total_line_total_int,
    grouping_id() as gid
from base_order_items_2 s
join orders d on s.order_id = d.id
group by cube (d.status)
order by rows_total desc nulls last;
