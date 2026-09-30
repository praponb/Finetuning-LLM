SELECT
    id,
    customer_id,
    status,
    channel,
    CORR(shipping_fee, amount) OVER (PARTITION BY channel) corr_shipping_fee_amount,
    LEAD(discount, 1) OVER (PARTITION BY status, customer_id ORDER BY amount ASC, id ASC) next_discount,
    PERCENT_RANK() OVER (PARTITION BY channel ORDER BY shipping_fee ASC) shipping_fee_percent_rank,
    RANK() OVER (PARTITION BY channel ORDER BY amount ASC NULLS LAST) amount_rank
FROM orders;
