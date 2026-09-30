select *
from (
    select
        id,
        store_id,
        amount,
        shipping_fee,
        discount
    from orders
) src
unpivot include nulls (
    metric_value for kpi in (amount as AMOUNT, shipping_fee, discount as DISCOUNT_VAL)
)
