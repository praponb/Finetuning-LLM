with base as (
    -- Keep recent, valid rows
    select
        *,
        cast(paid_at as date) as paid_at_day,
        coalesce(method, 'OTHER') as method_filled
    from payments
    where paid_at > timestamp '2023-10-03 14:00:00'
),
enriched as (
    -- Attach orders attributes
    select
        e.*,
        d.status as order_status,
        d.discount,
        d.amount as order_amount
    from base e
    left join orders d
        on e.order_id <=> d.id
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        concat_ws('/', status, order_status) as status_order_status_key,
        coalesce(method, 'UNKNOWN') as resolved_method
    from enriched
    where paid_at >= date '2023-05-29'
      and method is not null
),
enriched_payments as (
    -- Attach orders attributes
    select
        a.id,
        a.order_id,
        a.amount,
        a.paid_at_day,
        a.method_filled,
        a.order_status,
        a.discount,
        a.order_amount,
        b.order_date
    from filtered a
    left join orders b
        on a.order_id = b.id
)
select
    s.id,
    s.order_id,
    s.amount,
    s.order_status,
    s.order_date,
    case when order_status rlike '^(CANCELLED|PENDING)$' then 'CORE' when order_status rlike '^(COMPLETED|SHIPPED)$' then 'OTHER_ZONE' else 'PRIORITY' end as order_status_zone,
    (select min(o2.unit_price) from order_items o2 where o2.order_id = s.order_id) as min_order_item_unit_price
from enriched_payments s
