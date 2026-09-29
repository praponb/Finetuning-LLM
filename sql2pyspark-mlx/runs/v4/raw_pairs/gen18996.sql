with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        round(amount * 1.1, 2) as amount_adjusted,
        round(shipping_fee, 1) as shipping_fee_rounded
    from orders
    where channel is not null
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        store_id,
        amount,
        shipping_fee_rounded,
        shipping_fee,
        amount_adjusted
    from valid
) src
unpivot (
    metric_value for measure in (amount as AMOUNT, shipping_fee_rounded as SHIPPING_FEE_ROUNDED, shipping_fee as SHIPPING_FEE_VAL, amount_adjusted as AMOUNT_ADJUSTED_VAL)
);
