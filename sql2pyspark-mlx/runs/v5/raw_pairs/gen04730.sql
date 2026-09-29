with base as (
    -- Basic cleansing and defaults
    select *, quantity * unit_price as line_total
    from order_items
    where product_id is not null
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from base
    qualify row_number() over (partition by order_id order by quantity nulls last, id) = 1
),
joined as (
    -- Attach orders attributes
    select
        src.*,
        o.order_date,
        o.shipping_fee,
        o.status
    from deduped src
    left join orders o
        on src.order_id = o.id
    where o.customer_id in (26, 28, 34)
),
joined_order_items as (
    -- Attach orders attributes
    select
        src.*,
        b.amount,
        b.channel
    from joined src
    left join orders b
        on src.order_id = b.id
)
select
    id,
    order_id,
    quantity,
    unit_price,
    line_total,
    order_date,
    shipping_fee,
    status,
    case when status rlike '^(SHIPPED|COMPLETED)$' then 'PRIORITY' when status rlike '^(PENDING|CANCELLED)$' then 'GROUP_A' else 'EXTENDED' end as status_zone
from joined_order_items;
