with base as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        method,
        paid_at,
        status,
        length(method) as method_length,
        coalesce(method, 'DIRECT') as method_filled
    from payments
    where hour(paid_at) between 7 and 14
),
base_payments as (
    -- Keep recent, valid rows
    select
        *,
        case when method_length >= 86 then 'High' when method_length >= 18 then 'Medium' else 'Low' end as method_length_level,
        coalesce(method, 'DIRECT') as method_filled_2
    from base
    where method is not null
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from base_payments
    qualify row_number() over (partition by status order by paid_at asc nulls last, id asc) = 1
),
joined as (
    -- Attach orders attributes
    select
        src.*,
        b.amount,
        b.discount
    from latest src
    inner join orders b
        on src.order_id = b.id
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        method,
        paid_at,
        method_length,
        method_filled,
        method_length_level,
        method_filled_2,
        discount,
        concat_ws(',', method, status) as method_status_key,
        cast(paid_at as date) as paid_at_day,
        coalesce(method, 'UNKNOWN') as method_filled_3
    from joined
    where paid_at > date '2023-06-05'
      and method is not null
)
-- Running and moving aggregates
select
    id,
    order_id,
    method,
    paid_at,
    method_length,
    method_filled,
    method_length_level,
    method_filled_2,
    discount,
    method_status_key,
    paid_at_day,
    method_filled_3,
    first_value(method_length, true) over (order by method_length asc, id asc rows between unbounded preceding and current row) as first_known_method_length,
    lead(method_length, 1) over (order by paid_at asc, id asc) as next_method_length
from cleaned;
