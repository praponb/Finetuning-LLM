select *
from (
    select
        id,
        channel,
        amount,
        discount,
        shipping_fee
    from orders
) src
unpivot (
    val for kpi in (amount, discount, shipping_fee)
) long_rows
