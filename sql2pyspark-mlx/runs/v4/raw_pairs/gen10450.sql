with filtered as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        method,
        paid_at,
        concat(method, ', ', status) as method_status
    from payments
    where paid_at >= timestamp '2023-10-06 11:00:00'
      and method = 'card'
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by order_id order by paid_at desc nulls first, id asc) as latest_rank
        from filtered
    ) ranked
    where latest_rank = 1
),
enriched as (
    -- Attach orders attributes
    select
        src.*,
        d.order_date,
        d.discount
    from ranked src
    left join orders d
        on src.order_id = d.id
),
daily as (
    -- Multi-level aggregation
    select
        order_id,
        avg(discount) as avg_discount,
        max(discount) as max_discount,
        max(paid_at) as last_paid_at
    from enriched
    group by order_id
)
select
    s.order_id,
    s.max_discount,
    s.last_paid_at,
    (select max(t2.unit_price) from order_items t2 where t2.order_id = s.order_id) as max_order_item_unit_price
from daily s;
