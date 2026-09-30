with base as (
    -- Basic cleansing and defaults
    select
        *,
        upper(method) as method_upper,
        coalesce(amount, 0) as amount_filled,
        coalesce(method, 'DIRECT') as resolved_method
    from payments
    where paid_at >= to_date('2023-08-24')
),
remaining_keys as (
    -- Set difference on keys
    select order_id
    from base
    except
    select order_id
    from payments
    where paid_at >= '2025-01-24'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join remaining_keys k on s.order_id = k.order_id
),
joined as (
    -- Attach orders attributes
    select
        s.order_id,
        s.amount,
        s.paid_at,
        s.method_upper,
        s.amount_filled,
        s.resolved_method,
        d.discount
    from kept_rows s
    inner join orders d
        on s.order_id = d.id
    where d.amount > 550
),
with_windows as (
    -- Window metrics over several different partitions
    select
        order_id,
        resolved_method,
        percent_rank() over (partition by order_id order by amount) as amount_percent_rank
    from joined
)
select
    s.order_id,
    s.resolved_method,
    s.amount_percent_rank,
    (select min(x.unit_price) from order_items x where x.order_id = s.order_id) as min_order_item_unit_price
from with_windows s;
