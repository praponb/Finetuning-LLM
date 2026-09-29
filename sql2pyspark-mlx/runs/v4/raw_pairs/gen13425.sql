select *
from (
    select
        id,
        channel,
        discount,
        shipping_fee,
        amount
    from orders
) src
unpivot (
    metric_value for measure in (discount, shipping_fee, amount)
)
